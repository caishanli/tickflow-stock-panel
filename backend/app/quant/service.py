"""FastAPI 侧编排：提交回测(派生子进程)、账户管理、读库。"""
from __future__ import annotations

import datetime
import json
import os
import subprocess
import sys
import uuid

from . import db
from .config import CONFIG
from .datasource.manager import QuantDataProvider


def _script(name: str) -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "scripts", name
    )


_SIM_CHILD_TABLES = ("sim_state", "sim_equity_snapshots", "sim_trades", "sim_stop_loss")


def submit_backtest(params: dict) -> str:
    run_id = params.get("run_id") or uuid.uuid4().hex[:8]
    params = dict(params, run_id=run_id)
    db.insert_run(
        run_id,
        params.get("strategy_id", ""),
        params.get("name", ""),
        json.dumps(params, ensure_ascii=False),
        "queued",
    )
    subprocess.Popen(
        [sys.executable, _script("run_quant_backtest.py"), run_id],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
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
    db.update_sim_account(
        aid, status="running", started_at=datetime.datetime.now().isoformat()
    )
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    subprocess.Popen(
        [sys.executable, _script("run_quant_sim.py"), aid],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def account_pause(aid: str) -> None:
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w") as f:
        f.close()
    db.update_sim_account(aid, status="paused")


def account_reset(aid: str) -> None:
    db.update_sim_account(aid, status="created")
    pause = os.path.join(CONFIG.runtime_dir, f"{aid}.pause")
    if os.path.exists(pause):
        os.remove(pause)
    with db.get_conn() as c:
        for t in _SIM_CHILD_TABLES:
            c.execute(f"DELETE FROM {t} WHERE account_id=?", (aid,))


# ---- 读库封装（供 API 调用）----
def get_run(run_id: str):
    return db.get_run(run_id)


def list_runs(limit: int = 50):
    return db.list_runs(limit)


def get_run_equity(run_id: str):
    return db.get_equity(run_id)


def get_run_trades(run_id: str):
    return db.get_trades(run_id)


def get_run_logs(run_id: str):
    return db.get_logs(run_id)


def get_sim_account(aid: str):
    return db.get_sim_account(aid)


def list_sim_accounts():
    return db.list_sim_accounts()


def read_sim_state(aid: str):
    return db.read_sim_state(aid)


def get_sim_snapshots(aid: str):
    return db.get_sim_snapshots(aid)


def get_sim_trades(aid: str):
    return db.get_sim_trades(aid)


def get_sim_stoploss(aid: str):
    return db.get_sim_stoploss(aid)


def delete_run(run_id: str):
    return db.delete_run(run_id)
