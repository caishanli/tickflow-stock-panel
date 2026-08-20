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


def test_account_ensure_running_spawns_when_running_no_pause(tmp_quant, monkeypatch):
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


def test_account_ensure_running_logs_error_on_popen_failure(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    monkeypatch.setattr(service.subprocess, "Popen", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    service.account_ensure_running("a1")  # 不应 raise
    logs = db.get_sim_logs("a1")
    assert any(m["level"] == "error" for m in logs)