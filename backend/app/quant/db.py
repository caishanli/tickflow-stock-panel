"""量化模块独立 SQLite 库（data/quant.db）。

所有回测/模拟盘的收益、日志、交易记录落此库，与 tickflow 的
DuckDB/Parquet 数据层完全隔离。sqlite3 为标准库，无额外依赖。
"""
from __future__ import annotations

import json
import os
import sqlite3

from .config import CONFIG

_DB_PATH: str | None = None

_SCHEMA = """
CREATE TABLE IF NOT EXISTS backtest_runs (
    id TEXT PRIMARY KEY, strategy_id TEXT, name TEXT, params_json TEXT, status TEXT,
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
    strategy_id TEXT, start_date TEXT, frequency TEXT DEFAULT 'minute',
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
CREATE TABLE IF NOT EXISTS sim_logs (
    account_id TEXT, ts TEXT, level TEXT, message TEXT);
"""


def init_db(path: str | None = None) -> None:
    global _DB_PATH
    _DB_PATH = path or CONFIG.db_path
    conn = sqlite3.connect(_DB_PATH)
    try:
        conn.executescript(_SCHEMA)
        # 兼容旧库：若 backtest_runs 尚无 name 列则补加
        cols = {r[1] for r in conn.execute("PRAGMA table_info(backtest_runs)")}
        if "name" not in cols:
            conn.execute("ALTER TABLE backtest_runs ADD COLUMN name TEXT")
        # 兼容旧库：若尚无 pid 列则补加（M5：子进程 pid 落库，terminate/reset 按 pid 杀进程组）
        for table in ("backtest_runs", "sim_accounts"):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if "pid" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN pid INTEGER")
        # 兼容旧库：sim_accounts 补 strategy_id 列（策略驱动模拟盘绑定策略用）
        cols = {r[1] for r in conn.execute("PRAGMA table_info(sim_accounts)")}
        if "strategy_id" not in cols:
            conn.execute("ALTER TABLE sim_accounts ADD COLUMN strategy_id TEXT")
        # 兼容旧库：sim_accounts 补 start_date 列（开始模拟日期：早于今日则历史补跑）
        if "start_date" not in cols:
            conn.execute("ALTER TABLE sim_accounts ADD COLUMN start_date TEXT")
        # 兼容旧库：sim_accounts 补 frequency 列（运行频率 minute/daily）
        if "frequency" not in cols:
            conn.execute(
                "ALTER TABLE sim_accounts ADD COLUMN frequency TEXT DEFAULT 'minute'")
        conn.commit()
    finally:
        conn.close()


def get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_DB_PATH or CONFIG.db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(_DB_PATH or CONFIG.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---- 回测 ----
def insert_run(run_id, strategy_id, name, params_json, status="queued"):
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,name,params_json,status) VALUES(?,?,?,?,?)",
            (run_id, strategy_id, name, params_json, status),
        )


def upsert_run(run_id, strategy_id, name, params_json, status="running"):
    """插入回测记录；若 run_id 已存在（如 API 已建 'queued' 行）则更新，避免 UNIQUE 冲突。"""
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,name,params_json,status) VALUES(?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "status=excluded.status, strategy_id=excluded.strategy_id, name=excluded.name, "
            "params_json=excluded.params_json",
            (run_id, strategy_id, name, params_json, status),
        )


def update_run(run_id, status, metrics_json=None, error=None, finished_at=None):
    with get_conn() as c:
        c.execute(
            "UPDATE backtest_runs SET status=?, metrics_json=?, error=?, finished_at=? WHERE id=?",
            (status, metrics_json, error, finished_at, run_id),
        )


def set_run_pid(run_id, pid):
    """回测子进程 pid 落库（M5：terminate 按 pid 杀进程组）。"""
    with get_conn() as c:
        c.execute("UPDATE backtest_runs SET pid=? WHERE id=?", (pid, run_id))


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


def list_strategies_with_latest():
    """策略列表聚合：每个策略一行 + 最新一次回测的指标/周期/次数。"""
    with get_conn() as c:
        strat_rows = c.execute(
            "SELECT id, name FROM strategies ORDER BY updated_at DESC"
        ).fetchall()
        out = []
        for s in strat_rows:
            sid = s["id"]
            count = c.execute(
                "SELECT COUNT(*) AS n FROM backtest_runs WHERE strategy_id=?", (sid,)
            ).fetchone()["n"]
            latest = c.execute(
                "SELECT id, status, params_json, metrics_json, created_at "
                "FROM backtest_runs WHERE strategy_id=? ORDER BY created_at DESC LIMIT 1",
                (sid,),
            ).fetchone()
            item = {"id": sid, "name": s["name"], "run_count": count, "latest": None}
            if latest:
                p = {}
                try:
                    p = json.loads(latest["params_json"] or "{}")
                except Exception:
                    p = {}
                item["latest"] = {
                    "run_id": latest["id"],
                    "status": latest["status"],
                    "start": p.get("start"),
                    "end": p.get("end"),
                    "metrics_json": latest["metrics_json"],
                }
            out.append(item)
        return out


def bulk_insert_equity(run_id, rows):
    with get_conn() as c:
        c.executemany(
            "INSERT INTO backtest_equity(run_id,dt,value,benchmark,cash,positions_value) "
            "VALUES(?,?,?,?,?,?)",
            [(run_id, *r) for r in rows],
        )


def insert_equity_row(run_id, dt, value, benchmark, cash, positions_value):
    """单日收益实时写入（每日收盘钩子调用）。"""
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_equity(run_id,dt,value,benchmark,cash,positions_value) "
            "VALUES(?,?,?,?,?,?)",
            (run_id, dt, value, benchmark, cash, positions_value),
        )


def get_equity_after(run_id, offset=0):
    """返回 rowid > offset 的收益行（SSE 增量用）。"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, dt, value, benchmark, cash, positions_value "
            "FROM backtest_equity WHERE run_id=? AND rowid > ? ORDER BY rowid",
            (run_id, offset),
        ).fetchall()
    return [dict(r) for r in rows]


def get_max_equity_id(run_id):
    with get_conn() as c:
        row = c.execute(
            "SELECT MAX(rowid) AS m FROM backtest_equity WHERE run_id=?", (run_id,)
        ).fetchone()
    return row["m"] or 0


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


def get_trades_after(run_id, offset=0):
    """返回 rowid > offset 的成交行（SSE 增量用）。"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, ts, code, action, price, amount, pnl, pnl_pct, commission "
            "FROM backtest_trades WHERE run_id=? AND rowid > ? ORDER BY rowid",
            (run_id, offset),
        ).fetchall()
    return [dict(r) for r in rows]


def get_max_trade_id(run_id):
    with get_conn() as c:
        row = c.execute(
            "SELECT MAX(rowid) AS m FROM backtest_trades WHERE run_id=?", (run_id,)
        ).fetchone()
    return row["m"] or 0


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


def get_logs_after(run_id, offset=0):
    """返回 rowid > offset 的日志行（SSE 增量用）。"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, ts, level, message FROM backtest_logs "
            "WHERE run_id=? AND rowid > ? ORDER BY rowid",
            (run_id, offset),
        ).fetchall()
    return [dict(r) for r in rows]


def get_max_log_id(run_id):
    with get_conn() as c:
        row = c.execute(
            "SELECT MAX(rowid) AS m FROM backtest_logs WHERE run_id=?", (run_id,)
        ).fetchone()
    return row["m"] or 0


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
def insert_sim_account(account_id, name, capital, stop_loss, status="created",
                       strategy_id="", start_date="", frequency="minute"):
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_accounts(id,name,capital,stop_loss,status,strategy_id,"
            "start_date,frequency) VALUES(?,?,?,?,?,?,?,?)",
            (account_id, name, capital, stop_loss, status, strategy_id, start_date,
             frequency),
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
    """账户列表：联 sim_state 带最新净值/盈亏（列表页展示用，无状态行为 NULL）。"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT a.*, s.net_value AS net_value, s.pnl AS pnl "
            "FROM sim_accounts a LEFT JOIN sim_state s ON s.account_id = a.id "
            "ORDER BY a.created_at"
        ).fetchall()
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


def _json_or(raw, default):
    """JSON 解析容错：空串/坏串回退默认值，避免读状态崩溃。"""
    try:
        return json.loads(raw) if raw else default
    except (TypeError, ValueError):
        return default


def read_sim_state(account_id):
    """读取模拟盘 live 状态。

    M1：positions_json/stop_loss_log_json 解析成 positions/stop_loss_log 对象键，
    与 protocol.save_state 的写入侧命名对齐，保证 save→read 往返持仓不丢。
    """
    with get_conn() as c:
        row = c.execute("SELECT * FROM sim_state WHERE account_id=?", (account_id,)).fetchone()
    state = dict(row) if row else {
        "cash": 0.0, "positions_json": "{}", "net_value": 0.0, "pnl": 0.0,
        "start_cash": 0.0, "stop_loss_log_json": "[]", "dt": None,
    }
    state["positions"] = _json_or(state.get("positions_json"), {})
    state["stop_loss_log"] = _json_or(state.get("stop_loss_log_json"), [])
    return state


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


def insert_sim_log(account_id, ts, level, message):
    """模拟盘运行日志（策略 log.* 与 runner 事件）落库。"""
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_logs(account_id,ts,level,message) VALUES(?,?,?,?)",
            (account_id, ts, level, message),
        )


def get_sim_logs(account_id, limit=500):
    """按时间正序返回最近 limit 条日志。"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT ts,level,message FROM sim_logs WHERE account_id=? "
            "ORDER BY rowid DESC LIMIT ?",
            (account_id, limit),
        ).fetchall()
    return [dict(r) for r in reversed(rows)]


def get_sim_logs_after(account_id, offset=0):
    """返回 rowid > offset 的模拟盘日志（SSE 增量用）。"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, ts, level, message FROM sim_logs "
            "WHERE account_id=? AND rowid > ? ORDER BY rowid",
            (account_id, offset),
        ).fetchall()
    return [dict(r) for r in rows]


def get_sim_trades_after(account_id, offset=0):
    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, ts, code, action, price, amount, pnl, pnl_pct, commission "
            "FROM sim_trades WHERE account_id=? AND rowid > ? ORDER BY rowid",
            (account_id, offset),
        ).fetchall()
    return [dict(r) for r in rows]


def get_sim_snapshots_after(account_id, offset=0):
    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, dt, net_value, cash, positions_value, pnl, pnl_pct "
            "FROM sim_equity_snapshots WHERE account_id=? AND rowid > ? ORDER BY rowid",
            (account_id, offset),
        ).fetchall()
    return [dict(r) for r in rows]


def get_max_sim_log_id(account_id):
    with get_conn() as c:
        row = c.execute("SELECT MAX(rowid) AS m FROM sim_logs WHERE account_id=?", (account_id,)).fetchone()
    return row["m"] or 0


def get_max_sim_trade_id(account_id):
    with get_conn() as c:
        row = c.execute("SELECT MAX(rowid) AS m FROM sim_trades WHERE account_id=?", (account_id,)).fetchone()
    return row["m"] or 0


def get_max_sim_snapshot_id(account_id):
    with get_conn() as c:
        row = c.execute("SELECT MAX(rowid) AS m FROM sim_equity_snapshots WHERE account_id=?", (account_id,)).fetchone()
    return row["m"] or 0


def delete_sim_account(account_id):
    with get_conn() as c:
        c.execute("DELETE FROM sim_accounts WHERE id=?", (account_id,))
        for t in ("sim_state", "sim_equity_snapshots", "sim_trades", "sim_stop_loss",
                  "sim_logs"):
            c.execute(f"DELETE FROM {t} WHERE account_id=?", (account_id,))
