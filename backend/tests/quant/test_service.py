"""离线单元测试：patch 掉 subprocess.Popen，验证编排层行为。"""
from __future__ import annotations

import datetime
import os

import pytest

from app.quant import db
from app.quant.config import CONFIG
from app.quant import service


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    runtime = tmp_path / "quant_sim"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(runtime))
    db.init_db(str(db_path))
    return tmp_path


def test_account_create(tmp_quant):
    aid = service.account_create("acct1", 100000.0, 0.03)
    row = db.get_sim_account(aid)
    assert row is not None
    assert row["name"] == "acct1"
    assert row["capital"] == 100000.0
    assert row["stop_loss"] == 0.03


def test_submit_backtest(tmp_quant, monkeypatch):
    calls = []
    monkeypatch.setattr(
        subprocess_module(), "Popen", lambda *a, **k: calls.append((a, k)) or None
    )
    params = {
        "strategy_id": "",
        "symbols": ["600000.XSHG"],
        "start": "2024-01-02",
        "end": "2024-01-04",
        "frequency": "daily",
        "capital": 100000,
        "fee": 0.0003,
        "slippage": 0.001,
    }
    run_id = service.submit_backtest(params)
    row = db.get_run(run_id)
    assert row is not None
    assert row["status"] == "queued"
    assert db.get_run(run_id)["id"] == run_id
    assert len(calls) == 1
    args = calls[0][0][0]
    assert any("run_quant_backtest.py" in a for a in args)
    assert run_id in args


def test_account_pause(tmp_quant):
    aid = service.account_create("acct2", 50000.0, 0.05)
    service.account_pause(aid)
    row = db.get_sim_account(aid)
    assert row["status"] == "paused"
    assert os.path.exists(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"))


def test_account_reset(tmp_quant):
    aid = service.account_create("acct3", 200000.0, 0.02)
    # 填充子表
    db.upsert_sim_state(aid, 200000.0, "{}", 200000.0, 0.0, 200000.0, "[]", datetime.datetime.now().isoformat())
    db.insert_sim_snapshot(aid, "2024-01-02", 200000.0, 200000.0, 0.0, 0.0, 0.0)
    db.insert_sim_trade(aid, "2024-01-02", "600000.XSHG", "BUY", 10.0, 100, 0.0, 0.0, 0.0)
    db.insert_sim_stoploss(aid, "2024-01-02", "600000.XSHG", "浦发银行", "SELL", 10.0, 100, -50.0, -0.02, 0.0)

    service.account_reset(aid)

    row = db.get_sim_account(aid)
    assert row is not None, "账户行应被保留"
    assert row["name"] == "acct3"
    assert row["capital"] == 200000.0
    with db.get_conn() as c:
        for t in ("sim_state", "sim_equity_snapshots", "sim_trades", "sim_stop_loss"):
            n = c.execute(f"SELECT COUNT(*) AS n FROM {t} WHERE account_id=?", (aid,)).fetchone()["n"]
            assert n == 0, f"{t} 应被清空"


def subprocess_module():
    import subprocess

    return subprocess
