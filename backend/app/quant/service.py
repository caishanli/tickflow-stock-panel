"""FastAPI 侧编排：提交回测(派生子进程)、账户管理、读库。"""
from __future__ import annotations

import datetime
import glob
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

from . import db
from .config import CONFIG
from .datasource.manager import QuantDataProvider
from .simulate import memory as sim_memory

logger = logging.getLogger("app.quant.service")


def _script(name: str) -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "scripts", name
    )


_SIM_CHILD_TABLES = ("sim_state", "sim_equity_snapshots", "sim_trades", "sim_stop_loss",
                     "sim_logs")

# 串行化 account_start / account_ensure_running 的 spawn 决策，堵并发双 spawn。
# account_start 先置 running 后写 pid，窗口内 daemon sweep 会误判死 pid 而重复拉起；
# 用同一把锁 + pid 存活复核双保险（见 account_ensure_running 内注释）。
_spawn_lock = threading.Lock()


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


_BACKTEST_SPAWN_LOCK = threading.Lock()


def submit_backtest(params: dict, compile_mode: bool = False) -> str:
    if compile_mode:
        run_id = f"c_{uuid.uuid4().hex[:8]}"
        _sweep_compile_stale()
    else:
        run_id = params.get("run_id") or uuid.uuid4().hex[:8]
    params = dict(params, run_id=run_id)
    payload = json.dumps(params, ensure_ascii=False)

    # 并发/重复提交防护：同一 run_id 只允许一个活任务。
    # try_insert_run 原子占位（sqlite 写串行化，多进程下同时只有一个调用者
    # 拿到 True；同进程线程另有本锁串行化 spawn 全程）：
    # - 抢到占位 → 本次负责 spawn；
    # - 未抢到 → 读既有行：queued/running 且 pid 存活则拒绝（幂等报错，
    #   调用方据此去轮询既有 run，而不是再起一个 worker 导致双跑翻倍净值）；
    #   否则（queued 无进程、进程已死、终端态重跑）复位为 queued 后由本次接管。
    # compile run_id 恒为新 uuid，走正常占位 + spawn（缺了这步编译运行会静默
    # no-op：拿到 run_id 却永远没有 worker）。
    # 已知残留窗口：多进程下两个提交同时撞上"有行但无 pid"（对方刚占位、
    # Popen 未返回）时会双 spawn——窗口约一个 Popen 时长；单进程内本锁全闭合。
    # 同 run_id 跨进程并发属于病态调用（uuid 每次都不同），不值得加 token 栏。
    with _BACKTEST_SPAWN_LOCK:
        if not db.try_insert_run(
            run_id, params.get("strategy_id", ""), params.get("name", ""), payload, "queued"
        ):
            existing = db.get_run(run_id)
            if (
                existing
                and existing.get("status") in ("queued", "running")
                and _pid_alive(existing.get("pid"))
            ):
                raise RuntimeError(
                    f"run {run_id} 已在运行 (pid={existing.get('pid')})，拒绝重复提交")
            db.upsert_run(
                run_id, params.get("strategy_id", ""), params.get("name", ""),
                payload, "queued",
            )
        try:
            proc = subprocess.Popen(
                [sys.executable, _script("run_quant_backtest.py"), run_id],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as e:
            # M5：Popen 失败不留永久 queued 行 → 置 failed
            db.update_run(run_id, "failed", error=f"spawn failed: {e}")
            raise
        # M5：pid 落库，terminate 时按 pid 杀进程组（Popen 被 patch 时容错）
        pid = getattr(proc, "pid", None)
        if pid:
            db.set_run_pid(run_id, pid)
    return run_id


def _pid_alive(pid) -> bool:
    """pid 存活校验（os.kill(pid, 0) 不发信号，只校验进程存在）。

    PermissionError 表示进程存在、只是无权发信号 → 视为存活（此前误判为
    死亡，会导致活任务被重复拉起，正好与本函数目标相反）。
    """
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, ValueError, TypeError):
        # ValueError/TypeError：pid 非数字（如脏数据）→ 按死亡处理，
        # 调用方走接管分支而不是 500。
        return False


def _sweep_compile_stale(max_age_days: int = 7) -> None:
    """清扫 7 天前的编译库 .db 文件与编译 bundle 目录（quant_bundle/c_*）。"""
    cutoff = time.time() - max_age_days * 86400
    compile_dir = db.compile_dir()
    for f in (
        *glob.glob(os.path.join(compile_dir, "*.db")),
        *glob.glob(os.path.join(compile_dir, "*.db-wal")),
        *glob.glob(os.path.join(compile_dir, "*.db-shm")),
    ):
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


def terminate_backtest(run_id: str) -> None:
    """终止回测：先按 pid 杀子进程组（M5），再把 run 置 failed。

    注：桥接侧进程被杀后不得覆盖该终态（rqalpha_bridge 侧保证）。
    """
    run = db.get_run(run_id)
    if run:
        kill_process_group(run.get("pid"))
    db.update_run(run_id, "failed", error="terminated")


def account_create(name: str, capital: float, stop_loss: float, strategy_id: str = "",
                   start_date: str = "", frequency: str = "minute") -> str:
    aid = uuid.uuid4().hex[:8]
    db.insert_sim_account(aid, name, float(capital), float(stop_loss), "created",
                          strategy_id, start_date, frequency)
    return aid


def _guard_memory(aid: str) -> None:
    """手动启动的内存门禁：不足则 raise，不改状态、不写 pause。"""
    mem = sim_memory.memory_check(extra=1)
    if mem["ok"]:
        return
    raise ValueError(
        f"内存不足: 可用 {mem['available_mb']:.0f}MB < 需要 {mem['needed_mb']:.0f}MB"
        f"（每账户约 {mem['estimate_mb']:.0f}MB），已跳过启动"
    )


def account_start(aid: str) -> None:
    with _spawn_lock:
        acct = db.get_sim_account(aid)
        if not acct:
            raise ValueError(f"account not found: {aid}")
        if acct.get("status") == "running":
            return  # M4：幂等，运行中重复 start 不再拉起第二个进程
        _guard_memory(aid)
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


def account_ensure_running(aid: str) -> None:
    """守护自动拉起：仅当账户 status=running 且无 pause 文件时 spawn 子进程。

    与 account_start 不同——account_start 遇 running 幂等直接返回，无法重拉
    崩溃的账户。守护用此入口独立重拉：置 running、pid 落库、留日志；
    pause 文件存在（有意停止）时跳过，不清除。
    """
    with _spawn_lock:
        acct = db.get_sim_account(aid)
        if not acct or acct.get("status") != "running":
            return
        pause = os.path.join(CONFIG.runtime_dir, f"{aid}.pause")
        if os.path.exists(pause):
            return
        # 锁内复核 pid 存活：account_start 可能刚在同锁下拉起（running+新 pid），
        # 此时 pid 已落库且存活，直接放弃，避免与 account_start 双 spawn 竞态。
        from .simulate.daemon import _alive  # 延迟导入避免 daemon↔service 循环依赖

        if _alive(aid, acct.get("pid")):
            return
        mem = sim_memory.memory_check(extra=1)
        if not mem["ok"]:
            msg = (
                f"内存不足: 可用 {mem['available_mb']:.0f}MB < 需要 {mem['needed_mb']:.0f}MB"
                f"（每账户约 {mem['estimate_mb']:.0f}MB），未自动重启，稍后重试"
            )
            db.insert_sim_log(aid, str(datetime.datetime.now()), "warn", msg)
            logger.warning("sim account %s: %s", aid, msg)
            return
        os.makedirs(CONFIG.runtime_dir, exist_ok=True)
        db.update_sim_account(aid, started_at=datetime.datetime.now().isoformat())
        try:
            proc = subprocess.Popen(
                [sys.executable, _script("run_quant_sim.py"), aid],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as e:
            db.insert_sim_log(aid, str(datetime.datetime.now()), "error",
                              f"守护自动重启失败: {e}")
            return
        db.update_sim_account(aid, pid=getattr(proc, "pid", None))
        db.insert_sim_log(aid, str(datetime.datetime.now()), "warn",
                          "检测到进程退出，自动重启")


def account_pause(aid: str) -> None:
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w") as f:
        f.close()
    # 必须终止进程：只写 pause 文件时旧引擎仍驻留内存（数百MB），且后续
    # account_start 会另起新进程，形成同账户双进程（2026-08-25 ff8320ae
    # 双进程事故：新旧进程止损参数不一致，存在双重下单风险）。
    acct = db.get_sim_account(aid) or {}
    pid = acct.get("pid")
    if pid:
        try:
            os.kill(int(pid), 15)
        except (ProcessLookupError, ValueError):
            pass
        except Exception as e:
            logger.warning("account_pause kill %s failed: %s", pid, e)
    db.update_sim_account(aid, status="paused")


def account_reset(aid: str) -> None:
    acct = db.get_sim_account(aid) or {}
    # M4：先停活进程再清库，否则旧进程继续循环会把已删状态"复活"。
    # 先写 pause 再 kill：堵住 kill 后 DB 更新前 daemon 误拉起的窗口
    # （daemon 见 pause 文件会跳过）。pause 由下次 account_start 清除。
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w"):
        pass
    kill_process_group(acct.get("pid"))
    db.update_sim_account(aid, status="created", pid=None)
    with db.get_conn() as c:
        for t in _SIM_CHILD_TABLES:
            c.execute(f"DELETE FROM {t} WHERE account_id=?", (aid,))


def account_delete(aid: str) -> None:
    acct = db.get_sim_account(aid) or {}
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w"):
        pass
    kill_process_group(acct.get("pid"))
    db.delete_sim_account(aid)


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
