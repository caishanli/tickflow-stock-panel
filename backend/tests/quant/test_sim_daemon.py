"""模拟盘进程守护（SimDaemon）测试。隔离真实 store（tmp_path + CONFIG 覆盖）。"""
from __future__ import annotations

import types

import pytest

from app.quant import db, service
from app.quant.config import CONFIG


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path / "strategies"))
    db.init_db(str(db_path))
    return tmp_path


class _FakePopen:
    def __init__(self, args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.pid = 4242


def test_account_ensure_running_spawns_when_running_no_pause(tmp_quant, monkeypatch, mem_ok):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args[0][-1] == "a1"
    assert kwargs["start_new_session"] is True
    acct = db.get_sim_account("a1")
    assert acct["pid"] == 4242
    assert acct["status"] == "running"
    logs = db.get_sim_logs("a1")
    assert any("自动重启" in m["message"] for m in logs)


def test_account_ensure_running_skips_when_not_running(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "paused")
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert calls == []
    assert db.get_sim_account("a1")["pid"] is None


def test_account_ensure_running_skips_when_pause_file(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    pause = tmp_quant / "quant_sim" / "a1.pause"
    pause.parent.mkdir(parents=True, exist_ok=True)
    pause.write_text("")
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert calls == []


def test_account_ensure_running_skips_when_pid_alive(tmp_quant, monkeypatch):
    # 竞态回归：account_start 已拉起（pid 存活）时，守护不得重复 spawn。
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=4242)
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    from app.quant.simulate import daemon

    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: True)
    service.account_ensure_running("a1")
    assert calls == []


def test_account_ensure_running_spawns_when_pid_dead(tmp_quant, monkeypatch, mem_ok):
    # 反面分支：pid 已落库但进程死掉（_alive False）→ 仍应拉起。
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=4242)
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    from app.quant.simulate import daemon

    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: False)
    service.account_ensure_running("a1")
    assert len(calls) == 1


def test_account_ensure_running_logs_error_on_popen_failure(tmp_quant, monkeypatch, mem_ok):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    monkeypatch.setattr(service.subprocess, "Popen", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    service.account_ensure_running("a1")  # 不应 raise
    logs = db.get_sim_logs("a1")
    assert any(m["level"] == "error" for m in logs)


def test_account_ensure_running_skips_when_memory_insufficient(tmp_quant, monkeypatch):
    """内存守卫拦截路径: 不足时不 spawn、不落 pid, 落 warn 日志(钉住守卫行为)。"""
    from app.quant.simulate import memory as sim_memory
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)  # 钉住: 拦截路径不得改写 pid
    alive = types.SimpleNamespace(info={
        "pid": 1,
        "cmdline": ["python", "run_quant_sim.py", "other"],
        "memory_info": types.SimpleNamespace(rss=300 * 1024 ** 2),
    })
    monkeypatch.setattr(sim_memory.psutil, "process_iter", lambda attrs=(): iter([alive]))
    monkeypatch.setattr(sim_memory.psutil, "virtual_memory",
                        lambda: types.SimpleNamespace(available=100 * 1024 ** 2))
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert calls == []
    assert db.get_sim_account("a1")["pid"] == 12345
    logs = db.get_sim_logs("a1")
    assert any(m["level"] == "warn" and "内存不足" in m["message"] for m in logs)


def test_account_reset_writes_pause_even_when_kill_succeeds(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=9999)
    monkeypatch.setattr(service, "kill_process_group", lambda pid: True)
    monkeypatch.setattr(service, "_SIM_CHILD_TABLES", ())
    service.account_reset("a1")
    import os as _os
    assert _os.path.exists(tmp_quant / "quant_sim" / "a1.pause")


import os as _os

from app.quant.simulate import daemon


def test_alive_true_when_proc_and_cmdline_match(tmp_quant, monkeypatch):
    pid = _os.getpid()  # 当前进程存在，但 cmdline 不含 run_quant_sim
    assert daemon._alive("a1", pid) is False


def test_alive_false_when_pid_missing(tmp_quant, monkeypatch):
    assert daemon._alive("a1", 999999) is False


def test_alive_false_when_cmdline_mismatch(tmp_quant, monkeypatch):
    # 模拟 pid 被复用为其它进程：cmdline 不匹配 run_quant_sim.py {aid}
    pid = _os.getpid()
    monkeypatch.setattr(daemon, "_read_cmdline", lambda p: "python\x00other.py")
    assert daemon._alive("a1", pid) is False


def test_alive_true_when_cmdline_matches(tmp_quant, monkeypatch):
    pid = _os.getpid()
    monkeypatch.setattr(daemon, "_read_cmdline",
                        lambda p: "/usr/bin/python\x00/path/run_quant_sim.py\x00a1")
    monkeypatch.setattr(daemon.os.path, "exists", lambda p: True)
    assert daemon._alive("a1", pid) is True


def test_sweep_restarts_dead_running_no_pause(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    db.insert_sim_account("a2", "acc2", 100000.0, 0.03, "paused")
    db.insert_sim_account("a3", "acc3", 100000.0, 0.03, "failed")
    calls = []
    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: False)  # 全部判死
    monkeypatch.setattr(daemon.service, "account_ensure_running",
                        lambda aid: calls.append(aid))
    d = daemon.SimDaemon()
    d._sweep()
    assert calls == ["a1"]  # 仅 running 账户被拉起


def test_sweep_skips_when_pause_file(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    pause = tmp_quant / "quant_sim" / "a1.pause"
    pause.parent.mkdir(parents=True, exist_ok=True)
    pause.write_text("")
    calls = []
    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: False)
    monkeypatch.setattr(daemon.service, "account_ensure_running",
                        lambda aid: calls.append(aid))
    d = daemon.SimDaemon()
    d._sweep()
    assert calls == []


def test_sweep_skips_alive_process(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    calls = []
    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: True)
    monkeypatch.setattr(daemon.service, "account_ensure_running",
                        lambda aid: calls.append(aid))
    d = daemon.SimDaemon()
    d._sweep()
    assert calls == []