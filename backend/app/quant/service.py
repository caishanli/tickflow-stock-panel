"""FastAPI 侧编排：提交回测(派生子进程)、账户管理、读库。"""
from __future__ import annotations

import datetime
import json
import os
import signal
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


def kill_process_group(pid) -> bool:
    """按 pid 杀子进程组（子进程 start_new_session 独立会话，pid 即进程组 id）。

    进程已退出/无权限时不抛异常，返回 False。
    """
    if not pid:
        return False
    try:
        os.killpg(int(pid), signal.SIGTERM)
        return True
    except (ProcessLookupError, PermissionError, OSError):
        return False


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


def terminate_backtest(run_id: str) -> None:
    """终止回测：先按 pid 杀子进程组（M5），再把 run 置 failed。

    注：桥接侧进程被杀后不得覆盖该终态（rqalpha_bridge 侧保证）。
    """
    run = db.get_run(run_id)
    if run:
        kill_process_group(run.get("pid"))
    db.update_run(run_id, "failed", error="terminated")


def account_create(name: str, capital: float, stop_loss: float) -> str:
    aid = uuid.uuid4().hex[:8]
    db.insert_sim_account(aid, name, float(capital), float(stop_loss), "created")
    return aid


def account_start(aid: str) -> None:
    acct = db.get_sim_account(aid)
    if not acct:
        raise ValueError(f"account not found: {aid}")
    if acct.get("status") == "running":
        return  # M4：幂等，运行中重复 start 不再拉起第二个进程
    pause = os.path.join(CONFIG.runtime_dir, f"{aid}.pause")
    if os.path.exists(pause):
        os.remove(pause)
    db.update_sim_account(
        aid, status="running", started_at=datetime.datetime.now().isoformat()
    )
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    proc = subprocess.Popen(
        [sys.executable, _script("run_quant_sim.py"), aid],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    # M5：pid 落库，reset/终止时按 pid 杀进程组
    pid = getattr(proc, "pid", None)
    if pid:
        db.update_sim_account(aid, pid=pid)


def account_pause(aid: str) -> None:
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w") as f:
        f.close()
    db.update_sim_account(aid, status="paused")


def account_reset(aid: str) -> None:
    acct = db.get_sim_account(aid) or {}
    # M4：先停活进程再清库，否则旧进程继续循环会把已删状态"复活"
    if not kill_process_group(acct.get("pid")) and acct.get("status") == "running":
        # 无 pid 的旧进程：落 pause 文件让它下一轮自行退出（account_start 会清掉该文件）
        os.makedirs(CONFIG.runtime_dir, exist_ok=True)
        with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w"):
            pass
    db.update_sim_account(aid, status="created", pid=None)
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
