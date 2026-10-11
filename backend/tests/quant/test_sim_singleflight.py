"""分账户单实例文件锁测试（Task 6：重复拉起直接退出）。"""
from __future__ import annotations

import pytest

from app.quant import db
from app.quant.config import CONFIG


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path / "strategies"))
    db.init_db(str(db_path))
    return tmp_path


def test_second_acquire_same_account_raises(tmp_quant):
    from app.quant.simulate import singleflight

    h1 = singleflight.acquire("a1")
    try:
        with pytest.raises(singleflight.SimLockError):
            singleflight.acquire("a1")
    finally:
        singleflight.release(h1)


def test_release_then_acquire_ok(tmp_quant):
    from app.quant.simulate import singleflight

    h1 = singleflight.acquire("a1")
    singleflight.release(h1)
    h2 = singleflight.acquire("a1")
    singleflight.release(h2)


def test_run_loop_duplicate_exits_without_touching_status(tmp_quant):
    from app.quant.simulate import runner, singleflight

    db.insert_sim_account("a_dup", "a", 100000.0, 0.03, "running", "")
    h = singleflight.acquire("a_dup")
    try:
        runner.run_loop("a_dup")
    finally:
        singleflight.release(h)
    acct = db.get_sim_account("a_dup")
    assert acct is not None and acct["status"] == "running"
