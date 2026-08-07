# 量化模拟盘交易/持仓显示标的名字 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 模拟盘成交记录与持仓同时显示标的名字，并在落库时把名称存进 `sim_trades.name` 与持仓 dict 的 `name` 键。

**Architecture:** 根因是 stockdata 服务的 `DataSources.get_stock_names()` 返回空。先修好服务端（读本地 instruments parquet 股票名称 + 免费 TickFlow API 补 ETF 名称，进程内缓存），恢复策略名称分组；再在模拟盘子进程里加 `names.py` 客户端名称解析；DB `sim_trades` 加 `name` 列并落库时写入；前端展示名称。

**Tech Stack:** Python 3.11 / FastAPI / Polars / sqlite3 / React 18 + TS。

## Global Constraints

- 名称映射键约定：**纯 6 位代码 → 名称**（与 jqengine `get_all_securities` 里 `mootdx_names.get(pure, ...)` 查找约定一致，`api.py:890`）
- 服务端 `get_stock_names(codes=None)` 返回 `dict[str, str]`；`codes` 非空时只返回命中的子集
- 模拟盘子进程侧名称映射键：**JQ 码（`.XSHG`/`.XSHE`）→ 名称**
- 取不到名称的标的回退为代码本身
- 异常兜底：任何失败返回空映射/回退代码，不影响行情正确性
- 免费 TickFlow API 失败时降级：服务端返回已有本地名称（或空）
- DB 迁移沿用 `init_db` 的 `PRAGMA table_info` + `ALTER TABLE` 兼容模式（`db.py:60-83`）
- 后端测试从 `backend/` 目录运行：`uv run --extra dev pytest`
- 前端 lint/构建：`cd frontend && pnpm lint` / `pnpm build`

---

### Task 1: stockdata 服务端 `get_stock_names` 实现

**Files:**
- Modify: `backend/app/services/stockdata/sources.py:495`（`get_stock_names` 方法）
- Modify: `backend/app/services/stockdata/sources.py:261-274`（`__init__` 加缓存字段）
- Test: `backend/tests/quant/test_stockdata_sources.py`

**Interfaces:**
- Produces: `DataSources.get_stock_names(codes: list[str] | None = None) -> dict[str, str]`，键为纯 6 位代码（如 `"159985"`），值为名称（如 `"豆粕ETF华夏"`）。供 Task 2 的客户端 `StockDataClient.get_stock_names()` 透传，及 jqengine `get_all_securities` 直接消费。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_stockdata_sources.py` 末尾追加（放在现有 `test_metadata_methods_with_ohlcv_only_partitions` 之后）：

```python
def _write_instruments(root, rows):
    """写 instruments parquet（股票名称本地来源）。"""
    import os
    import polars as pl
    d = os.path.join(root, "instruments")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame(rows).write_parquet(os.path.join(d, "instruments.parquet"))


def test_get_stock_names_returns_local_stock_names(tmp_path, monkeypatch):
    """股票名称来自本地 instruments parquet；API 失败时仍有本地名称。"""
    import os
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    _write_instruments(str(tmp_path), [
        {"symbol": "600000.SH", "name": "浦发银行", "code": "600000"},
        {"symbol": "600249.SH", "name": "两面针", "code": "600249"},
    ])
    # 免费 API 必失败（未 mock）→ 降级本地
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=1)
    try:
        names = s.get_stock_names()
        assert names.get("600000") == "浦发银行"
        assert names.get("600249") == "两面针"
        # codes 子集过滤
        sub = s.get_stock_names(codes=["600000"])
        assert sub == {"600000": "浦发银行"}
    finally:
        s.puller.shutdown()
        os.environ.pop("PARTITION_DATA_ROOT", None)


def test_get_stock_names_etf_falls_back_to_api(tmp_path, monkeypatch):
    """ETF 名称本地 parquet 为空时走免费 API；异常时返回已有股票名称。"""
    import os
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    _write_instruments(str(tmp_path), [
        {"symbol": "600000.SH", "name": "浦发银行", "code": "600000"},
    ])
    # mock ETF instruments 来源返回空 + 抛异常（模拟 API 失败降级）
    monkeypatch.setattr(
        "app.services.index_sync._fetch_instruments_by_type",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no net")),
    )
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=1)
    try:
        names = s.get_stock_names()
        assert names.get("600000") == "浦发银行"  # 本地名称仍在
    finally:
        s.puller.shutdown()
        os.environ.pop("PARTITION_DATA_ROOT", None)


def test_get_stock_names_etf_from_local_parquet(tmp_path, monkeypatch):
    """ETF 名称若本地 instruments_etf parquet 已有则直接读，不触网。"""
    import os
    import polars as pl
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    _write_instruments(str(tmp_path), [
        {"symbol": "600000.SH", "name": "浦发银行", "code": "600000"},
    ])
    d = os.path.join(str(tmp_path), "instruments_etf")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame([
        {"symbol": "159985.SZ", "name": "豆粕ETF华夏", "code": "159985"},
        {"symbol": "511880.SH", "name": "银华日利ETF", "code": "511880"},
    ]).write_parquet(os.path.join(d, "instruments_etf.parquet"))
    # API 调用若发生会抛异常 → 测试断言它没被调用
    def _boom(*a, **k):
        raise AssertionError("ETF 名称应从本地 parquet 读，不调 API")
    monkeypatch.setattr(
        "app.services.index_sync._fetch_instruments_by_type", _boom)
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=1)
    try:
        names = s.get_stock_names()
        assert names.get("159985") == "豆粕ETF华夏"
        assert names.get("511880") == "银华日利ETF"
        assert names.get("600000") == "浦发银行"
    finally:
        s.puller.shutdown()
        os.environ.pop("PARTITION_DATA_ROOT", None)
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py::test_get_stock_names_returns_local_stock_names -v`
Expected: FAIL（`names` 为空 dict，`assert names.get("600000") == "浦发银行"` 抛断言错误）

- [ ] **Step 3: 实现**

修改 `backend/app/services/stockdata/sources.py`：

在文件顶部 import 区加：

```python
import json as _json
```

在 `DataSources.__init__`（:261-274）末尾加缓存字段：

```python
        self._names_map: dict[str, str] | None = None
        self._names_cache_file = os.path.join(self.data_root, ".stock_names_cache.json")
```

替换 `get_stock_names`（:495-497）为：

```python
    def _build_name_map(self) -> dict[str, str]:
        """构建 {纯6位代码: 名称} 映射：本地 instruments（股票）+ ETF（本地或免费 API）。

        名称属展示层：任何失败降级为空/部分映射，不影响行情路径。
        """
        out: dict[str, str] = {}
        # 1) 股票：本地 instruments parquet（免费档已含全量股票名称）
        try:
            inst = os.path.join(self.data_root, "instruments", "instruments.parquet")
            if os.path.exists(inst):
                df = pl.read_parquet(inst)
                if "symbol" in df.columns and "name" in df.columns:
                    for sym, name in df.select(["symbol", "name"]).iter_rows():
                        if sym and name:
                            out[str(sym).split(".")[0]] = str(name)
        except Exception:
            logger.warning("get_stock_names: instruments 读取失败", exc_info=True)
        # 2) ETF：本地 instruments_etf parquet 优先，缺失则免费 TickFlow API 补
        try:
            import glob as _glob
            etf_paths = _glob.glob(
                os.path.join(self.data_root, "instruments_etf", "**", "*.parquet"),
                recursive=True)
            df_etf = None
            if etf_paths:
                try:
                    df_etf = pl.scan_parquet(etf_paths).collect()
                except Exception:
                    df_etf = None
            if df_etf is None or df_etf.is_empty() or "name" not in df_etf.columns:
                from app.services.index_sync import _fetch_instruments_by_type
                df_etf = _fetch_instruments_by_type("etf", "etf")
            if df_etf is not None and not df_etf.is_empty() \
                    and "symbol" in df_etf.columns and "name" in df_etf.columns:
                for sym, name in df_etf.select(["symbol", "name"]).iter_rows():
                    if sym and name:
                        out.setdefault(str(sym).split(".")[0], str(name))
        except Exception:
            logger.warning("get_stock_names: ETF 名称获取失败，降级本地", exc_info=True)
        # 3) 落盘缓存（下次启动命中，免网络）
        try:
            os.makedirs(os.path.dirname(self._names_cache_file), exist_ok=True)
            with open(self._names_cache_file, "w", encoding="utf-8") as f:
                _json.dump(out, f, ensure_ascii=False)
        except Exception:
            pass
        return out

    def get_stock_names(self, codes: list[str] | None = None) -> dict[str, str]:
        """返回 {纯6位代码: 名称} 映射；codes 非空时只返回命中的子集。

        恢复 jqengine get_all_securities/get_security_name 的名称解析，
        同时为模拟盘落库提供名称。进程内缓存，首次构建后复用。
        """
        if self._names_map is None:
            self._names_map = self._build_name_map()
        if not codes:
            return dict(self._names_map)
        return {c: n for c, n in self._names_map.items() if c in set(codes)}
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py -v`
Expected: 全绿（含新增 3 个 + 原有测试；`test_metadata_methods_with_ohlcv_only_partitions` 里 `assert src.get_stock_names() == {}` 需确认——该测试的 tmp_path 无 instruments parquet，名称映射为空，仍通过）

- [ ] **Step 5: 提交**

```bash
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_sources.py
git commit -m "feat(stockdata): 实现 get_stock_names 返回股票/ETF 名称，恢复策略名称分组"
```

---

### Task 2: 模拟盘子进程名称解析模块 `names.py`

**Files:**
- Create: `backend/app/quant/simulate/names.py`
- Test: `backend/tests/quant/test_simulate_names.py`（新建）

**Interfaces:**
- Consumes: `StockDataClient.get_stock_names()`（`backend/app/quant/datasource/network_client.py:182`，返回 `{纯6位代码: 名称}`）
- Produces:
  - `get_name_map() -> dict[str, str]`：进程内缓存 `{JQ码: 名称}`；`{JQ码: 名称}` 供 Task 3/4 用
  - `resolve_name(code: str) -> str`：查映射，缺失回退 `code`

- [ ] **Step 1: 写失败测试**

创建 `backend/tests/quant/test_simulate_names.py`：

```python
"""模拟盘进程名称解析模块测试。"""
from __future__ import annotations

from app.quant.simulate import names


def test_resolve_name_uses_client_map(monkeypatch):
    calls = []

    def _fake_get_stock_names(self, codes=None):
        calls.append(codes)
        return {"159985": "豆粕ETF华夏", "600000": "浦发银行"}

    monkeypatch.setattr(
        "app.quant.datasource.network_client.StockDataClient.get_stock_names",
        _fake_get_stock_names,
    )
    # 清模块级缓存，强制重建
    names._NAMES = None
    try:
        nm = names.get_name_map()
        assert nm.get("159985.XSHE") == "豆粕ETF华夏"
        assert nm.get("600000.XSHG") == "浦发银行"
        # 解析：JQ 码命中
        assert names.resolve_name("159985.XSHE") == "豆粕ETF华夏"
        # 未命中回退代码
        assert names.resolve_name("999999.XSHG") == "999999.XSHG"
        # 进程内缓存：第二次不再调 client
        names.get_name_map()
        assert len(calls) == 1
    finally:
        names._NAMES = None


def test_get_name_map_empty_on_error(monkeypatch):
    def _boom(self, codes=None):
        raise RuntimeError("service down")

    monkeypatch.setattr(
        "app.quant.datasource.network_client.StockDataClient.get_stock_names",
        _boom,
    )
    names._NAMES = None
    try:
        assert names.get_name_map() == {}
        assert names.resolve_name("600000.XSHG") == "600000.XSHG"
    finally:
        names._NAMES = None


def test_resolve_name_none_map_fallback():
    assert names.resolve_name("159985.XSHE") == "159985.XSHE"
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_simulate_names.py -v`
Expected: FAIL（`ModuleNotFoundError: app.quant.simulate.names`）

- [ ] **Step 3: 实现**

创建 `backend/app/quant/simulate/names.py`：

```python
"""模拟盘进程内标的名称解析。

名称来源：stockdata 服务 get_stock_names（客户端 StockDataClient 透传），
返回 {纯6位代码: 名称}；本模块转成 {JQ码: 名称} 并在进程内缓存。
任何失败降级为空映射 → resolve_name 回退代码，不影响行情正确性。
"""
from __future__ import annotations

import logging

log = logging.getLogger("app.quant.simulate.names")

_NAMES: dict[str, str] | None = None  # {JQ码: 名称}


def _to_jq(pure: str, symbol: str) -> str:
    """纯代码 + 分区符号(.SH/.SZ) -> JQ码(.XSHG/.XSHE)。"""
    suffix = symbol.rsplit(".", 1)[-1]
    return pure + (".XSHG" if suffix in ("SH", "XSHG") else ".XSHE")


def get_name_map() -> dict[str, str]:
    """返回 {JQ码: 名称}，进程内缓存。失败返回空映射。"""
    global _NAMES
    if _NAMES is not None:
        return _NAMES
    out: dict[str, str] = {}
    try:
        from ..datasource.network_client import StockDataClient
        client = StockDataClient()
        # 服务端返回 {纯6位代码: 名称}，无分区符号 → 无法直接转 JQ 后缀。
        # 因此客户端映射键直接保留纯代码，resolve_name 按纯代码查。
        raw = client.get_stock_names() or {}
        for pure, name in raw.items():
            if name:
                out[pure] = str(name)
    except Exception:
        log.warning("get_stock_names 失败，标的名称回退代码", exc_info=True)
    _NAMES = out
    return out


def resolve_name(code: str) -> str:
    """按标的代码（JQ 码或纯代码）查名称，缺失回退代码本身。"""
    pure = code.split(".", 1)[0]
    return get_name_map().get(pure) or code
```

注意：映射键用**纯代码**（与服务端键一致），`resolve_name` 按 `code.split(".")[0]` 查——比转 JQ 后缀更稳（无需知道标的所属市场）。

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_simulate_names.py -v`
Expected: 全绿（3 个测试）

- [ ] **Step 5: 提交**

```bash
git add backend/app/quant/simulate/names.py backend/tests/quant/test_simulate_names.py
git commit -m "feat(sim): 新增名称解析模块 names.py（stockdata 服务名称 + 进程内缓存）"
```

---

### Task 3: DB schema 加 `sim_trades.name` 列

**Files:**
- Modify: `backend/app/quant/db.py:42-44`（建表 SQL）
- Modify: `backend/app/quant/db.py:54-86`（`init_db` 迁移）
- Modify: `backend/app/quant/db.py:499-514`（`insert_sim_trade`/`get_sim_trades`）
- Modify: `backend/app/quant/db.py:467-476`（`batch_insert_trades`）
- Modify: `backend/app/quant/db.py:572-579`（`get_sim_trades_after`）
- Test: `backend/tests/quant/test_db.py`

**Interfaces:**
- Produces:
  - `insert_sim_trade(account_id, ts, code, action, price, amount, pnl, pnl_pct, commission, name="")`
  - `batch_insert_trades(rows)`：rows 每项为 `(account_id, ts, code, action, price, amount, pnl, pnl_pct, commission, name)`
  - `get_sim_trades(account_id)` / `get_sim_trades_after(account_id, offset=0)` 返回行含 `name` 键
- Consumes: Task 4（runner `_persist`）用这些新签名。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_db.py` 追加：

```python
def test_sim_trade_name_column():
    p = _fresh()
    db.insert_sim_trade("a1", "2024-01-02 09:31", "159985.XSHE", "BUY",
                        2.139, 100, 0.0, 0.0, 9.99, "豆粕ETF华夏")
    trades = db.get_sim_trades("a1")
    assert len(trades) == 1
    assert trades[0]["code"] == "159985.XSHE"
    assert trades[0]["name"] == "豆粕ETF华夏"
    db.delete_sim_account("a1")
    os.unlink(p)


def test_sim_trade_name_optional_old_signature():
    """不带 name 的旧调用仍可用（name 默认空串）。"""
    p = _fresh()
    db.insert_sim_trade("a1", "2024-01-02 09:31", "600000.XSHG", "BUY", 10.0, 100, 0.0, 0.0, 0.0)
    trades = db.get_sim_trades("a1")
    assert trades[0]["name"] == ""
    db.delete_sim_account("a1")
    os.unlink(p)


def test_sim_trade_batch_with_name():
    p = _fresh()
    db.batch_insert_trades([
        ("a1", "2024-01-02 09:31", "159985.XSHE", "BUY", 2.139, 100, 0.0, 0.0, 9.99, "豆粕ETF华夏"),
        ("a1", "2024-01-02 09:32", "511880.XSHG", "SELL", 100.0, 1000, 0.0, 0.0, 1.0, "银华日利ETF"),
    ])
    trades = db.get_sim_trades("a1")
    assert len(trades) == 2
    assert {t["name"] for t in trades} == {"豆粕ETF华夏", "银华日利ETF"}
    db.delete_sim_account("a1")
    os.unlink(p)


def test_sim_trade_name_column_migration_on_old_db():
    """旧库（无 name 列）init_db 自动补列，历史行 name 为 NULL。"""
    p = _fresh()
    import sqlite3
    # 手动建无 name 列的旧表结构（先删新表）
    conn = sqlite3.connect(p)
    conn.execute("DROP TABLE sim_trades")
    conn.execute(
        "CREATE TABLE sim_trades (account_id TEXT, ts TEXT, code TEXT, action TEXT, "
        "price REAL, amount REAL, pnl REAL, pnl_pct REAL, commission REAL)")
    conn.execute(
        "INSERT INTO sim_trades VALUES('a1','2024-01-02 09:31','600000.XSHG','BUY',"
        "10.0,100,0.0,0.0,0.0)")
    conn.commit(); conn.close()
    # init_db 补列
    db.init_db(p)
    trades = db.get_sim_trades("a1")
    assert len(trades) == 1
    assert "name" in trades[0]
    assert trades[0]["name"] is None
    os.unlink(p)
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_db.py::test_sim_trade_name_column -v`
Expected: FAIL（`insert_sim_trade` 拒绝第 10 个位置参数）

- [ ] **Step 3: 实现**

修改 `backend/app/quant/db.py`：

`_SCHEMA` 中 `sim_trades` 建表（:42-44）加 `name` 列：

```python
CREATE TABLE IF NOT EXISTS sim_trades (
    account_id TEXT, ts TEXT, code TEXT, name TEXT, action TEXT, price REAL, amount REAL,
    pnl REAL, pnl_pct REAL, commission REAL);
```

`init_db`（:59 `conn.executescript(_SCHEMA)` 之后，加迁移）追加：

```python
        # 兼容旧库：sim_trades 补 name 列（标的名称落库）
        cols = {r[1] for r in conn.execute("PRAGMA table_info(sim_trades)")}
        if "name" not in cols:
            conn.execute("ALTER TABLE sim_trades ADD COLUMN name TEXT")
```

替换 `insert_sim_trade`（:499-505）为：

```python
def insert_sim_trade(account_id, ts, code, action, price, amount, pnl, pnl_pct,
                     commission, name=""):
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_trades(account_id,ts,code,name,action,price,amount,pnl,pnl_pct,commission) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (account_id, ts, code, name, action, price, amount, pnl, pnl_pct, commission),
        )
```

替换 `batch_insert_trades`（:467-476）为：

```python
def batch_insert_trades(rows):
    """批量写入成交。rows: list of (account_id, ts, code, action, price, amount,
    pnl, pnl_pct, commission, name)"""
    if not rows:
        return
    with get_conn() as c:
        c.executemany(
            "INSERT INTO sim_trades(account_id,ts,code,name,action,price,amount,pnl,pnl_pct,commission) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
```

替换 `get_sim_trades`（:508-514）为：

```python
def get_sim_trades(account_id):
    with get_conn() as c:
        rows = c.execute(
            "SELECT ts,code,name,action,price,amount,pnl,pnl_pct,commission FROM sim_trades "
            "WHERE account_id=? ORDER BY ts", (account_id,)
        ).fetchall()
    return [dict(r) for r in rows]
```

替换 `get_sim_trades_after`（:572-579）的 SELECT 加 `name`：

```python
def get_sim_trades_after(account_id, offset=0):
    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, ts, code, name, action, price, amount, pnl, pnl_pct, commission "
            "FROM sim_trades WHERE account_id=? AND rowid > ? ORDER BY rowid",
            (account_id, offset),
        ).fetchall()
    return [dict(r) for r in rows]
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_db.py -v`
Expected: 全绿（含 4 个新测试 + 现有测试，`test_sim_account_and_state` 里 `insert_sim_trade` 旧签名仍可用）

- [ ] **Step 5: 提交**

```bash
git add backend/app/quant/db.py backend/tests/quant/test_db.py
git commit -m "feat(sim): sim_trades 表加 name 列 + 迁移 + 读写函数"
```

---

### Task 4: 模拟盘 runner 落库时写名称

**Files:**
- Modify: `backend/app/quant/simulate/runner.py:429-450`（`_persist`）
- Modify: `backend/app/quant/simulate/runner.py:305-314`（`_state_from_portfolio`）
- Test: `backend/tests/quant/test_fix_sim.py` 或新建 `backend/tests/quant/test_sim_names_write.py`

**Interfaces:**
- Consumes:
  - `names.resolve_name(code: str) -> str`（Task 2）
  - `db.insert_sim_trade(..., name="")` / `db.batch_insert_trades(rows_with_name)`（Task 3）
- Produces: 落库的 `sim_trades.name` 与 `sim_state.positions_json` 每个持仓 dict 含 `name` 键。

- [ ] **Step 1: 写失败测试**

创建 `backend/tests/quant/test_sim_names_write.py`：

```python
"""模拟盘落库写名称测试：成交带 name、持仓带 name。"""
from __future__ import annotations

import pytest

from app.quant import db
from app.quant.config import CONFIG
from app.quant.simulate import names, runner


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    db.init_db(str(db_path))
    return tmp_path


def test_persist_trade_row_carries_name(tmp_quant, monkeypatch):
    """_persist 构造的 trade_row 第 4 位（name）来自 names.resolve_name。"""
    monkeypatch.setattr(names, "get_name_map",
                        lambda: {"159985": "豆粕ETF华夏"})
    class _Api:
        _state = {"trades": [
            {"dt": "2024-01-02 09:31", "code": "159985.XSHE",
             "amount": 100, "price": 2.139, "fee": 9.99},
        ]}
    aux = {"trades_drained": 0, "replay_mode": False}
    state = {"start_cash": 100000.0}
    ctx = _FakeCtx()
    runner._persist("a1", ctx, state, "2024-01-02 09:31", _Api(), aux)
    trades = db.get_sim_trades("a1")
    assert len(trades) == 1
    assert trades[0]["code"] == "159985.XSHE"
    assert trades[0]["name"] == "豆粕ETF华夏"


def test_state_from_portfolio_positions_carry_name(tmp_quant, monkeypatch):
    monkeypatch.setattr(names, "get_name_map",
                        lambda: {"511880": "银华日利ETF"})
    class _P:
        def __init__(self, amount, avg_cost, price):
            self.amount = amount; self.avg_cost = avg_cost
            self.price = price; self.today_amount = 0.0
    class _Ctx:
        portfolio = type("PF", (), {
            "positions": {"511880.XSHG": _P(1000, 1.0, 1.1)},
        })()
    state = {}
    runner._state_from_portfolio(_Ctx(), state)
    assert state["positions"]["511880.XSHG"]["name"] == "银华日利ETF"


class _FakeCtx:
    """最小 ctx：_persist 只读 portfolio.cash/positions。"""
    portfolio = type("PF", (), {"cash": 100000.0, "positions": {}})()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_sim_names_write.py -v`
Expected: FAIL（`trades[0]["name"]` 不存在，KeyError 或 None；持仓无 `name` 键）

- [ ] **Step 3: 实现**

修改 `backend/app/quant/simulate/runner.py`：

顶部 import 区（:18 附近 `from .matcher import Matcher` 后）加：

```python
from . import names
```

`_state_from_portfolio`（:305-314）持仓 dict 加 `name`：

```python
def _state_from_portfolio(ctx, state: dict) -> dict:
    """portfolio → 旧 state dict（供 Matcher 巡检与 protocol.save_state 落库）。"""
    state["positions"] = {
        code: {
            "amount": float(p.amount), "avg_cost": float(p.avg_cost),
            "price": float(p.price),
            "today_amount": float(getattr(p, "today_amount", 0.0) or 0.0),
            "name": names.resolve_name(code),
        }
        for code, p in ctx.portfolio.positions.items()
    }
    state["cash"] = float(ctx.portfolio.cash)
    return state
```

`_persist`（:442-445）trade_row 增加 name（放在最后，匹配 `insert_sim_trade` 第 10 参）：

```python
        trade_row = (account_id, str(t["dt"]),
                     t["code"], "BUY" if t["amount"] > 0 else "SELL",
                     t["price"], amount, round(pnl, 4), round(pnl_pct, 4),
                     t.get("fee", 0.0), names.resolve_name(t["code"]))
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_sim_names_write.py -v`
Expected: 全绿

再跑 runner 相关回归确认没破坏主循环：

Run: `uv run --extra dev pytest tests/quant/test_runner_strategy.py tests/quant/test_fix_sim.py -q`
Expected: 全绿

- [ ] **Step 5: 提交**

```bash
git add backend/app/quant/simulate/runner.py backend/tests/quant/test_sim_names_write.py
git commit -m "feat(sim): runner 落库成交/持仓写入标的名字"
```

---

### Task 5: API 透传 `name` + 迁移脚本

**Files:**
- Modify: `backend/app/quant/api/quant.py:413-416`（`sim_stream` trade 事件）
- Create: `backend/scripts/backfill_sim_names.py`
- Test: `backend/tests/quant/test_api_quant.py`（追加 SSE 事件断言）；迁移脚本手动验证

**Interfaces:**
- Consumes: Task 3 的 `db.get_sim_trades_after` 返回行含 `name`
- Produces: SSE `trade` 事件 data 含 `name` 键；迁移脚本可独立执行回填历史数据。

- [ ] **Step 1: 写失败测试（SSE 事件含 name）**

在 `backend/tests/quant/test_api_quant.py` 追加：

```python
def test_sim_stream_trade_event_includes_name(tmp_quant, api_client):
    """SSE trade 事件透传 sim_trades.name。"""
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "created")
    db.insert_sim_trade("a1", "2024-01-02 09:31", "159985.XSHE", "BUY",
                        2.139, 100, 0.0, 0.0, 9.99, "豆粕ETF华夏")
    with api_client.stream("GET", "/sim/accounts/a1/stream", timeout=3) as r:
        lines = [ln for ln in r.iter_lines() if ln]
    text = "\n".join(lines)
    assert "event: trade" in text
    assert "豆粕ETF华夏" in text
```

若 `test_api_quant.py` 无 `tmp_quant`/`api_client` fixture，参考 `test_runner_strategy.py:18-34` 复制这两个 fixture（`tmp_quant` 用 `tmp_path` + monkeypatch `CONFIG.db_path`；`api_client` 用 `FastAPI` + `router` + `TestClient`）。

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_api_quant.py::test_sim_stream_trade_event_includes_name -v`
Expected: FAIL（SSE trade 事件 data 无 `name` 键，`豆粕ETF华夏` 不在 text 中）

- [ ] **Step 3: 实现**

修改 `backend/app/quant/api/quant.py` `sim_stream` 的 trade 事件（:413-416）：

```python
                for row in db.get_sim_trades_after(aid, off_trade):
                    off_trade = row["rowid"]
                    d = {k: row[k] for k in ("ts", "code", "name", "action", "price",
                                             "amount", "pnl", "pnl_pct", "commission")}
                    yield f"event: trade\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
```

创建 `backend/scripts/backfill_sim_names.py`：

```python
#!/usr/bin/env python3
"""一次性回填模拟盘历史数据：sim_trades.name + 持仓 name 键。

幂等，可重复执行：只补 name 为空的行/持仓。
用法：cd backend && uv run --extra dev python scripts/backfill_sim_names.py
"""
from __future__ import annotations

import json

from app.quant import db
from app.quant.simulate import names


def main() -> None:
    db.init_db()
    name_map = names.get_name_map()
    total_trades = 0
    total_pos = 0
    for acct in db.list_sim_accounts():
        aid = acct["id"]
        # 1) 成交记录
        with db.get_conn() as c:
            rows = c.execute(
                "SELECT rowid, code, name FROM sim_trades "
                "WHERE account_id=? AND (name IS NULL OR name='')",
                (aid,),
            ).fetchall()
            for r in rows:
                n = name_map.get(r["code"].split(".")[0]) or r["code"]
                c.execute("UPDATE sim_trades SET name=? WHERE rowid=?",
                          (n, r["rowid"]))
            total_trades += len(rows)
        # 2) 持仓
        st = db.read_sim_state(aid)
        pos = st.get("positions") or {}
        changed = False
        for code, p in pos.items():
            if not p.get("name"):
                p["name"] = name_map.get(code.split(".")[0]) or code
                changed = True
        if changed:
            st["positions_json"] = json.dumps(pos, ensure_ascii=False)
            db.upsert_sim_state(
                aid, float(st.get("cash", 0.0)),
                st["positions_json"], float(st.get("net_value", 0.0)),
                float(st.get("pnl", 0.0)), float(st.get("start_cash", 0.0)),
                json.dumps(st.get("stop_loss_log", []), ensure_ascii=False),
                st.get("dt"))
            total_pos += 1
    print(f"backfill done: trades={total_trades}, accounts_with_positions={total_pos}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_api_quant.py::test_sim_stream_trade_event_includes_name -v`
Expected: PASS

迁移脚本手动验证（不连网）：

```bash
cd backend && uv run --extra dev python -c "
from app.quant import db
db.init_db()
from app.quant.simulate import names
names._NAMES = {'600000': '浦发银行'}
import importlib, app.quant.simulate.names as m
import sys; sys.modules['app.quant.simulate.names'] = m
"
```

如环境无网，直接确认脚本可被 import 且语法正确即可（`python -m py_compile scripts/backfill_sim_names.py`）。

- [ ] **Step 5: 提交**

```bash
git add backend/app/quant/api/quant.py backend/scripts/backfill_sim_names.py backend/tests/quant/test_api_quant.py
git commit -m "feat(sim): API/SSE 透传 name + 历史数据回填脚本"
```

---

### Task 6: 前端显示名称

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx:519`（成交记录标的列）
- Modify: `frontend/src/quant/pages/QuantSim.tsx:470`（持仓表标的列）

**Interfaces:**
- Consumes: trade 对象含 `name` 字段（Task 3/5 透传）；持仓 dict 含 `name` 键（Task 4 落库）
- Produces: 前端成交记录标的列显示 `名称 代码`，持仓表显示名称。

- [ ] **Step 1: 修改成交记录标的列**

`frontend/src/quant/pages/QuantSim.tsx` 成交记录标的列（:519）：

```tsx
<td className="px-3 py-1.5">
  {t.name ? `${t.name} ` : ''}{t.code ?? ''}
</td>
```

- [ ] **Step 2: 修改持仓表标的列**

持仓表标的列（:470）当前为 `<td className="px-3 py-1.5">{sym}</td>`，改为显示名称 + 代码：

```tsx
<td className="px-3 py-1.5">
  {p.name ? `${p.name} ` : ''}{sym}
</td>
```

（`p` 是持仓 dict，含 `name` 键；`sym` 是代码键。）

- [ ] **Step 3: 前端 lint + 构建**

Run: `cd frontend && pnpm lint`
Expected: 通过

Run: `cd frontend && pnpm build`
Expected: 构建成功（`tsc -b && vite build`）

- [ ] **Step 4: 提交**

```bash
git add frontend/src/quant/pages/QuantSim.tsx
git commit -m "feat(sim): 前端成交记录与持仓显示标的名字"
```

---

## Self-Review

**Spec coverage:**
- Task 1 → spec 改动清单 §1（服务端 `get_stock_names`）+ 恢复策略名称分组
- Task 2 → spec §2（`names.py`）
- Task 3 → spec §3（DB schema）
- Task 4 → spec §4（runner 写入侧）
- Task 5 → spec §5（迁移脚本）+ §6（API 透传）
- Task 6 → spec §7（前端）
- 影响面/验收 → 各任务测试覆盖

**Placeholder scan:** 无 TBD/TODO；每步有完整代码与命令。

**Type consistency:** `resolve_name(code)` 按纯代码查（Task 2），Task 4/5 均用
`code.split(".")[0]` 或直接传 code——一致。`insert_sim_trade` name 为第 10 参数
（Task 3），Task 4 trade_row 第 10 位是 name——一致。SSE 事件字段 `name`（Task 5）
与前端 `t.name`/`p.name`（Task 6）——一致。
