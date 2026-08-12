"""API 响应缓存头测试：/api/ 动态响应必须禁止浏览器缓存。

无 Cache-Control 时 Chrome 会启发式缓存 GET 响应，前端拿到过期快照
（模拟盘日志/成交只显示到某一天、刷新才恢复）。SSE 流已有 no-cache，不覆盖。
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

from app.main import api_no_cache_middleware


def _make_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(BaseHTTPMiddleware, dispatch=api_no_cache_middleware)

    @app.get("/api/quant/sim/accounts")
    def accounts():
        return {"data": []}

    @app.get("/api/quant/sim/accounts/a1/logs")
    def logs():
        return {"data": []}

    @app.get("/health")
    def health():
        return {"ok": True}

    return app


def test_api_responses_have_no_store_header():
    client = TestClient(_make_app())
    for path in ("/api/quant/sim/accounts", "/api/quant/sim/accounts/a1/logs"):
        r = client.get(path)
        assert r.status_code == 200
        assert r.headers.get("cache-control") == "no-store", path


def test_non_api_responses_untouched():
    client = TestClient(_make_app())
    r = client.get("/health")
    assert r.status_code == 200
    assert "cache-control" not in r.headers
