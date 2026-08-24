"""模拟盘启动内存守卫：spawn run_quant_sim.py 前检查系统空闲内存。

估算口径：单账户 = max(活模拟盘进程 RSS 均值, SIM_ACCOUNT_MEM_MIN_MB)；
无活进程样本时回退 SIM_ACCOUNT_MEM_MB。需要 = 估算 × (活进程数 + 本次新增)。
"""
from __future__ import annotations

import psutil

from ..config import CONFIG

_SIM_SCRIPT = "run_quant_sim.py"


def _is_sim_proc(cmdline) -> bool:
    return bool(cmdline) and any(c.endswith(_SIM_SCRIPT) for c in cmdline)


def list_alive_sim_procs() -> list[dict]:
    """当前系统里所有模拟盘子进程：[{pid, rss_mb}]（cmdline 匹配，不绑定具体账户）。"""
    out: list[dict] = []
    for p in psutil.process_iter(["cmdline", "memory_info"]):
        try:
            if not _is_sim_proc(p.info.get("cmdline")):
                continue
            rss = p.info.get("memory_info")
            rss_mb = (rss.rss if rss else 0) / 1024**2
            out.append({"pid": p.info.get("pid"), "rss_mb": rss_mb})
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return out


def _estimate_mb(procs: list[dict]) -> float:
    if not procs:
        return CONFIG.sim_account_mem_mb
    mean = sum(p["rss_mb"] for p in procs) / len(procs)
    return max(mean, CONFIG.sim_account_mem_min_mb)


def estimate_per_account_mb() -> float:
    return _estimate_mb(list_alive_sim_procs())


def memory_check(extra: int = 1) -> dict:
    """内存门禁：ok=False 表示不应再 spawn。

    extra = 本次拟新增的账户数（默认 1）。available 用 MemAvailable 口径。
    宽限：系统还有虚拟内存（swap）可兜底，容量按原口径放宽 1 个账户——
    needed = estimate × max(extra, 活进程数 + extra - 1)。
    """
    procs = list_alive_sim_procs()
    available_mb = psutil.virtual_memory().available / 1024**2
    estimate_mb = _estimate_mb(procs)
    needed_mb = estimate_mb * max(extra, len(procs) + extra - 1)
    return {
        "ok": available_mb >= needed_mb,
        "available_mb": available_mb,
        "needed_mb": needed_mb,
        "estimate_mb": estimate_mb,
        "alive": len(procs),
    }