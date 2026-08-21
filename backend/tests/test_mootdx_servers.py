"""GET /api/data/mootdx-servers 端点。"""
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import data as api


def _make_app():
    app = FastAPI()
    app.include_router(api.router)
    return TestClient(app)


def _probe_stub():
    return [{"ip": "1.1.1.1", "port": 7709, "ok": True, "latency_ms": 12},
            {"ip": "2.2.2.2", "port": 7709, "ok": False, "latency_ms": None}]


def test_mootdx_servers_endpoint(monkeypatch):
    monkeypatch.setattr(api, "_mootdx_probe", lambda: _probe_stub())
    r = _make_app().get("/api/data/mootdx-servers")
    assert r.status_code == 200
    body = r.json()
    assert body["servers"] == _probe_stub()
    assert "ts" in body


def test_mootdx_probe_cache(monkeypatch):
    """真实 _mootdx_probe 的 TTL 缓存逻辑: 新鲜命中, 过期重新探测。"""
    from app.quant.jqengine.datasource import mootdx_src as msrc
    calls = {"n": 0}

    def counting():
        calls["n"] += 1
        return [{"ip": "1.1.1.1", "port": 7709, "ok": True, "latency_ms": 12}]

    monkeypatch.setattr(msrc, "probe_servers", counting)
    monkeypatch.setattr(api, "_mootdx_probe_cache", {"ts": 0.0, "data": None})
    now = [100.0]
    monkeypatch.setattr(api.time, "monotonic", lambda: now[0])

    assert api._mootdx_probe() == [{"ip": "1.1.1.1", "port": 7709, "ok": True, "latency_ms": 12}]
    assert calls["n"] == 1
    now[0] = 105.0
    assert api._mootdx_probe() == [{"ip": "1.1.1.1", "port": 7709, "ok": True, "latency_ms": 12}]
    assert calls["n"] == 1
    now[0] = 115.0
    assert api._mootdx_probe() == [{"ip": "1.1.1.1", "port": 7709, "ok": True, "latency_ms": 12}]
    assert calls["n"] == 2