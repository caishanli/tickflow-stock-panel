"""模拟盘启动内存守卫测试：估算口径 + 门禁 + 两入口拦截行为。

mock psutil（process_iter / virtual_memory），隔离真实 DB/CONFIG。
"""
from __future__ import annotations

import types

import pytest

from app.quant import db, service
from app.quant.config import CONFIG
from app.quant.simulate import memory as sim_memory


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path / "strategies"))
    db.init_db(str(db_path))
    return tmp_path


class _FakeRSS:
    def __init__(self, rss_bytes):
        self.rss = rss_bytes


class _FakeProc:
    def __init__(self, pid, cmdline, rss_mb):
        self.info = {
            "pid": pid,
            "cmdline": cmdline,
            "memory_info": _FakeRSS(int(rss_mb * 1024**2)),
        }


def _no_procs(monkeypatch):
    monkeypatch.setattr(sim_memory.psutil, "process_iter", lambda attrs=(): iter([]))


def _procs(monkeypatch, *rss_mb_list):
    items = [_FakeProc(1000 + i, ["python", "run_quant_sim.py", f"a{i}"], mb)
             for i, mb in enumerate(rss_mb_list)]
    monkeypatch.setattr(sim_memory.psutil, "process_iter",
                        lambda attrs=(): iter(items))


def _available(monkeypatch, mb):
    monkeypatch.setattr(
        sim_memory.psutil, "virtual_memory",
        lambda: types.SimpleNamespace(available=mb * 1024**2),
    )


# ---- 估算口径 ----

def test_estimate_fallback_default_when_no_procs(monkeypatch):
    _no_procs(monkeypatch)
    assert sim_memory.estimate_per_account_mb() == CONFIG.sim_account_mem_mb


def test_estimate_floor_when_mean_below_min(monkeypatch):
    monkeypatch.setattr(CONFIG, "sim_account_mem_min_mb", 300.0)
    _procs(monkeypatch, 100.0, 120.0)  # 均值 110 < 下限
    assert sim_memory.estimate_per_account_mb() == 300.0


def test_estimate_uses_mean_when_above_floor(monkeypatch):
    _procs(monkeypatch, 400.0, 600.0)
    assert sim_memory.estimate_per_account_mb() == 500.0


# ---- 门禁 ----

def test_memory_check_blocks_when_insufficient(monkeypatch):
    _procs(monkeypatch, 300.0)  # 1 个活进程
    _available(monkeypatch, 200.0)
    m = sim_memory.memory_check(extra=1)
    assert m["ok"] is False
    assert m["alive"] == 1
    assert m["estimate_mb"] == 300.0
    assert m["needed_mb"] == 600.0  # 300 × (1 活 + 1 新增)


def test_memory_check_passes_when_sufficient(monkeypatch):
    _procs(monkeypatch, 300.0)
    _available(monkeypatch, 1000.0)
    assert sim_memory.memory_check(extra=1)["ok"] is True


# ---- account_start 拦截 ----

class _FakePopen:
    def __init__(self, args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.pid = 4242


def _block_memory(monkeypatch):
    monkeypatch.setattr(
        service.sim_memory, "memory_check",
        lambda extra=1: {"ok": False, "available_mb": 200.0, "needed_mb": 600.0,
                         "estimate_mb": 300.0, "alive": 0},
    )


def _ok_memory(monkeypatch):
    monkeypatch.setattr(
        service.sim_memory, "memory_check",
        lambda extra=1: {"ok": True, "available_mb": 1000.0, "needed_mb": 600.0,
                         "estimate_mb": 300.0, "alive": 0},
    )


def test_account_start_raises_and_keeps_state(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "created")
    _block_memory(monkeypatch)
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append(1) or _FakePopen(*a, **k))
    with pytest.raises(ValueError, match="内存不足"):
        service.account_start("a1")
    assert calls == []
    acct = db.get_sim_account("a1")
    assert acct["status"] == "created"  # 不被置 running
    assert acct["pid"] is None
    assert not (tmp_quant / "quant_sim" / "a1.pause").exists()


def test_account_start_proceeds_when_memory_ok(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "created")
    _ok_memory(monkeypatch)
    monkeypatch.setattr(service.subprocess, "Popen", _FakePopen)
    service.account_start("a1")
    acct = db.get_sim_account("a1")
    assert acct["status"] == "running"
    assert acct["pid"] == 4242


# ---- account_ensure_running / daemon ----

def test_ensure_running_skips_and_logs_warn_when_memory_low(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    from app.quant.simulate import daemon

    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: False)
    _block_memory(monkeypatch)
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append(1) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert calls == []
    logs = db.get_sim_logs("a1")
    assert any(m["level"] == "warn" and "内存不足" in m["message"] for m in logs)


def test_sweep_respects_memory_guard(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    from app.quant.simulate import daemon

    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: False)
    _block_memory(monkeypatch)
    d = daemon.SimDaemon()
    d._sweep()
    acct = db.get_sim_account("a1")
    assert acct["pid"] == 12345  # 未 spawn，pid 不被覆盖
    logs = db.get_sim_logs("a1")
    assert any(m["level"] == "warn" and "内存不足" in m["message"] for m in logs)