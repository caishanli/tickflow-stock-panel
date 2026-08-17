"""Task 11 离线测试：FastAPI TestClient 验证 quant router（不派生子进程 worker）。

通过 monkeypatch 将 CONFIG.db_path 等指向临时目录、并 patch submit_backtest /
account_start 以避免真实 rqalpha 子进程，并验证各端点返回 200/404。
"""
from __future__ import annotations

import os
import sys
import tempfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.quant import db


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # 真实模块在导入即读取 .env；这里用环境变量强制指向临时库与目录
    monkeypatch.setenv("QUANT_DB_PATH", str(tmp_path / "quant.db"))
    monkeypatch.setenv("QUANT_BUNDLE_DIR", str(tmp_path / "bundle"))
    monkeypatch.setenv("QUANT_STRATEGIES_DIR", str(tmp_path / "strategies"))
    monkeypatch.setenv("QUANT_RUNTIME_DIR", str(tmp_path / "runtime"))

    from app.quant import db, config
    from app.quant import service

    # 不写真实库 / 不 spawn worker（显式指向临时库，隔离 import 顺序影响）
    config.CONFIG.db_path = str(tmp_path / "quant.db")
    db.init_db()
    db._COMPILE_DIR = str(tmp_path / "compile")

    # patch 掉会派生子进程的编排函数
    monkeypatch.setattr(
        service, "submit_backtest",
        lambda params, compile_mode=False: db.insert_run(
            ("c_" if compile_mode else "") + (params.get("run_id") or "run123"),
            params.get("strategy_id", ""),
            params.get("name", ""),
            __import__("json").dumps(params, ensure_ascii=False),
            "queued",
        ) or ("c_" if compile_mode else "") + (params.get("run_id") or "run123"),
    )
    monkeypatch.setattr(service, "account_start", lambda aid: db.update_sim_account(
        aid, status="running", started_at="2026-01-01T00:00:00"))

    from app.quant.api.quant import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_strategies_list_and_create(client):
    r = client.get("/api/quant/strategies")
    assert r.status_code == 200
    assert r.json()["data"] == []

    r = client.post("/api/quant/strategies", json={"name": "s1", "code": "print(1)"})
    assert r.status_code == 200
    sid = r.json()["data"]["id"]
    assert r.json()["data"]["name"] == "s1"

    r = client.get("/api/quant/strategies")
    assert r.status_code == 200
    assert any(s["id"] == sid for s in r.json()["data"])

    # 单一策略可取回代码
    r = client.get(f"/api/quant/strategies/{sid}")
    assert r.status_code == 200
    assert r.json()["data"]["code"] == "print(1)"


def test_datasource_priority(client):
    r = client.get("/api/quant/datasource")
    assert r.status_code == 200
    assert "priority" in r.json()["data"]

    r = client.post("/api/quant/datasource/priority",
                    json={"priority": ["astock", "tushare"]})
    assert r.status_code == 200
    r = client.get("/api/quant/datasource")
    assert r.json()["data"]["priority"] == ["astock", "tushare"]


def test_sim_accounts(client):
    r = client.get("/api/quant/sim/accounts")
    assert r.status_code == 200
    assert r.json()["data"] == []

    r = client.post("/api/quant/sim/accounts",
                    json={"name": "acc1", "capital": 100000, "stop_loss": 0.05})
    assert r.status_code == 200
    row = r.json()["data"]
    assert row["name"] == "acc1"
    assert row["capital"] == 100000
    aid = row["id"]

    r = client.get("/api/quant/sim/accounts")
    assert r.status_code == 200
    assert any(a["id"] == aid for a in r.json()["data"])

    # start 仅更新 DB 状态（worker 已 patch）
    r = client.post(f"/api/quant/sim/accounts/{aid}/start")
    assert r.status_code == 200


def test_backtest_missing_returns_404(client):
    r = client.delete("/api/quant/backtest/nope")
    assert r.status_code == 404
    r = client.get("/api/quant/backtest/nope/status")
    assert r.status_code == 404


def test_backtest_run(client):
    r = client.post("/api/quant/backtest/run",
                    json={"strategy_code": "x", "symbols": ["600000"],
                          "start": "2024-01-01", "end": "2024-02-01"})
    assert r.status_code == 200
    body = r.json()["data"]
    assert body["run_id"]
    assert body["status"] == "queued"

    run_id = body["run_id"]
    r = client.get(f"/api/quant/backtest/{run_id}/status")
    assert r.status_code == 200
    assert r.json()["data"]["id"] == run_id


def test_sim_status_returns_trade_days(client, monkeypatch):
    import types

    import app.quant.datasource.network_client as nc
    from app.quant import db

    db.insert_sim_account("a1", "acc", 100000.0, 0.03, "created",
                          strategy_id="", start_date="2026-01-05")
    days = ["2026-01-05", "2026-01-06", "2026-01-07"]
    fake = types.SimpleNamespace(get_trade_days=lambda s, e: days)
    monkeypatch.setattr(nc, "StockDataClient", lambda: fake)

    r = client.get("/api/quant/sim/accounts/a1/status")
    assert r.status_code == 200
    assert r.json()["data"]["trade_days"] == days


def test_build_trade_days_network_failure_fallback(monkeypatch):
    import app.quant.datasource.network_client as nc
    from app.quant.api import quant as qmod

    class Boom:
        def __init__(self):
            raise RuntimeError("network down")

    monkeypatch.setattr(nc, "StockDataClient", Boom)

    acct = {"start_date": "2026-08-03"}  # 周一，当天为工作日
    days = qmod._build_trade_days(acct, [])
    assert days and days[0] == "2026-08-03"
    assert all(len(d) == 10 for d in days)


def test_backtest_record_flag(client):
    r = client.post("/api/quant/backtest/run",
                    json={"name": "n", "strategy_id": "s1", "start": "2024-01-02",
                          "end": "2024-01-03", "record": False})
    assert r.status_code == 200
    assert r.json()["data"]["run_id"].startswith("c_")
    r2 = client.post("/api/quant/backtest/run",
                     json={"name": "n", "strategy_id": "s1", "start": "2024-01-02",
                           "end": "2024-01-03"})
    assert r2.status_code == 200
    assert not r2.json()["data"]["run_id"].startswith("c_")
    rows = client.get("/api/quant/backtest/runs").json()["data"]
    assert all(not x["id"].startswith("c_") for x in rows)


def test_backtest_record_not_in_params_json(client):
    r = client.post("/api/quant/backtest/run",
                    json={"name": "n", "strategy_id": "s1", "record": False})
    row = db.get_run(r.json()["data"]["run_id"])
    assert "record" not in (row["params_json"] or "")


def test_worker_routes_compile_db_path(monkeypatch, tmp_path):
    import importlib.util
    path = "scripts/run_quant_backtest.py"
    spec = importlib.util.spec_from_file_location("run_quant_backtest_c", path)
    rb = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rb)
    params = {"strategy_id": "", "run_id": "c_12345678", "start": "2020-01-01",
              "end": "2020-02-01", "symbols": ["600000.XSHG"]}
    captured = {}

    def fake_get_run(run_id):
        return {"params_json": __import__("json").dumps(params)}

    def fake_run_backtest(code, params, provider=None, db_path=None):
        captured["db_path"] = db_path
        return {"run_id": "c_12345678"}

    monkeypatch.setattr(rb.db, "get_run", fake_get_run)
    monkeypatch.setattr(rb.db, "_COMPILE_DIR", str(tmp_path / "compile"))
    monkeypatch.setattr(rb, "run_backtest", fake_run_backtest)
    old = sys.argv
    sys.argv = ["run_quant_backtest.py", "c_12345678"]
    try:
        rb.main()
    finally:
        sys.argv = old
    assert captured["db_path"].endswith("c_12345678.db")


def test_backtest_trades_includes_name(client):
    db.insert_run("r1", "s1", "n1", "{}", "done")
    db.insert_trade("r1", "2026-08-17 10:30:00", "600000.XSHG", "SIDE.BUY", 10.0, 100.0, 0.0, 0.0, 1.0)
    r = client.get("/api/quant/backtest/r1/trades")
    assert r.status_code == 200
    rows = r.json()["data"]
    assert len(rows) == 1
    row = rows[0]
    assert row["code"] == "600000.XSHG"
    assert "name" in row
    assert row["name"]
