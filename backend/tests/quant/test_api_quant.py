"""Task 11 离线测试：FastAPI TestClient 验证 quant router（不派生子进程 worker）。

通过 monkeypatch 将 CONFIG.db_path 等指向临时目录、并 patch submit_backtest /
account_start 以避免真实 rqalpha 子进程，并验证各端点返回 200/404。
"""
from __future__ import annotations

import os
import tempfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


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

    # patch 掉会派生子进程的编排函数
    monkeypatch.setattr(
        service, "submit_backtest",
        lambda params: db.insert_run(
            (params.get("run_id") or "run123"),
            params.get("strategy_id", ""),
            params.get("name", ""),
            __import__("json").dumps(params, ensure_ascii=False),
            "queued",
        ) or "run123",
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
