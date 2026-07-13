"""量化模块独立 SQLite 库（data/quant.db）。

所有回测/模拟盘的收益、日志、交易记录落此库，与 tickflow 的
DuckDB/Parquet 数据层完全隔离。sqlite3 为标准库，无额外依赖。
"""
from __future__ import annotations

import os
import sqlite3

from .config import CONFIG

_DB_PATH: str | None = None

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
    global _DB_PATH
    _DB_PATH = path or CONFIG.db_path
    conn = sqlite3.connect(_DB_PATH)
    try:
        conn.executescript(_SCHEMA)
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
def insert_run(run_id, strategy_id, params_json, status="queued"):
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,params_json,status) VALUES(?,?,?,?)",
            (run_id, strategy_id, params_json, status),
        )


def upsert_run(run_id, strategy_id, params_json, status="running"):
    """插入回测记录；若 run_id 已存在（如 API 已建 'queued' 行）则更新，避免 UNIQUE 冲突。"""
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,params_json,status) VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "status=excluded.status, strategy_id=excluded.strategy_id, params_json=excluded.params_json",
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
        c.execute("DELETE FROM sim_accounts WHERE id=?", (account_id,))
        for t in ("sim_state", "sim_equity_snapshots", "sim_trades", "sim_stop_loss"):
            c.execute(f"DELETE FROM {t} WHERE account_id=?", (account_id,))
