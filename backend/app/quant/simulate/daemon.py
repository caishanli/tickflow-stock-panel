"""模拟盘进程守护：运行中账户（status=running）子进程挂了自动拉起。

由主后端 lifespan 启动后台守护线程，周期性扫描 sim_accounts。
仅拉起 status=running 且 pid 已死且无 pause 文件的账户（pause=有意停止）。
存活判定用 /proc/{pid}/cmdline 匹配 run_quant_sim.py {account_id} 防 pid 复用。
"""
from __future__ import annotations

import logging
import os
import threading

from .. import db, service
from ..config import CONFIG

logger = logging.getLogger("app.quant.simulate.daemon")


def _read_cmdline(pid) -> str:
    """读取 /proc/{pid}/cmdline（以 \x00 分隔拼接）；失败返回空串。"""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().decode(errors="ignore")
    except OSError:
        return ""


def _alive(aid: str, pid) -> bool:
    """进程是否确为本账户的模拟盘进程（防 pid 复用）。

    /proc/{pid}/cmdline 参数间以 \\x00 分隔，脚本路径可能是绝对路径；
    统一转空白后按 token 匹配：存在 run_quant_sim.py 且 aid 是独立 token。
    """
    if not pid:
        return False
    if not os.path.exists(f"/proc/{pid}"):
        return False
    tokens = _read_cmdline(pid).replace("\x00", " ").split()
    return any(t.endswith("run_quant_sim.py") for t in tokens) and aid in tokens


class SimDaemon:
    """守护线程：周期扫描 running 账户，死 pid 自动拉起。"""

    def __init__(self, poll_interval: float = 10.0):
        self._poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _should_restart(self, acct: dict) -> bool:
        aid = acct["id"]
        if acct.get("status") != "running":
            return False
        if _alive(aid, acct.get("pid")):
            return False
        return not os.path.exists(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"))

    def _sweep(self) -> None:
        try:
            for acct in db.list_sim_accounts():
                if self._should_restart(acct):
                    logger.warning("sim account %s dead, auto restarting", acct["id"][:8])
                    service.account_ensure_running(acct["id"])
        except Exception:
            logger.exception("[sim-daemon] sweep 异常，下轮重试")

    def _watch(self) -> None:
        self._sweep()  # 首扫
        while not self._stop.wait(self._poll_interval):
            self._sweep()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._watch, name="sim-daemon",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
