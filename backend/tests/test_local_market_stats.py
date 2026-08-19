"""本地股市数据统计端点测试。"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import data as api


def _write_partition(root: Path, sub: str, d: str, symbols: list[str]) -> None:
    """写一个 date=YYYY-MM-DD 分区, 含 symbol 列。"""
    part = root / sub / f"date={d}"
    part.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "symbol": symbols,
        "date": [date.fromisoformat(d)] * len(symbols),
    })
    df.write_parquet(part / "part.parquet")


class _FakeRepo:
    """最小 repo: data_dir + 真实 duckdb 执行 SQL (SQL 内 read_parquet 读真实文件)。"""

    def __init__(self, data_dir: Path):
        self.store = SimpleNamespace(data_dir=data_dir)
        self._db = duckdb.connect()

    def execute_one(self, sql: str, params: list | None = None) -> tuple | None:
        return self._db.execute(sql, params or []).fetchone()


def _make_app(repo: _FakeRepo) -> FastAPI:
    app = FastAPI()
    app.include_router(api.router)
    app.state.repo = repo
    return app


@pytest.fixture
def repo(tmp_path: Path) -> _FakeRepo:
    root = tmp_path / "data"
    root.mkdir()
    _write_partition(root, "kline_daily", "2026-08-14", ["000001.SZ", "600000.SH", "000002.SZ"])
    _write_partition(root, "kline_daily", "2026-08-17", ["000001.SZ", "600000.SH"])
    _write_partition(root, "kline_minute", "2026-08-17", ["000001.SZ", "600000.SH"])
    _write_partition(root, "kline_etf_daily", "2026-08-17", ["510050.SH", "159001.SZ"])
    _write_partition(root, "kline_etf_minute", "2026-08-17", ["510050.SH"])
    _write_partition(root, "kline_index_daily", "2026-08-17", ["000001.SH"])
    # 故意不建 kline_index_minute 目录
    return _FakeRepo(root)


def test_counts_per_date(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    r = client.get("/api/data/local-market-stats?page=1&page_size=15")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2
    assert [row["date"] for row in body["rows"]] == ["2026-08-17", "2026-08-14"]
    newest = body["rows"][0]
    assert newest["stock_daily"] == 2
    assert newest["stock_minute"] == 2
    assert newest["etf_daily"] == 2
    assert newest["etf_minute"] == 1
    assert newest["index_daily"] == 1
    assert newest["index_minute"] == 0  # 目录不存在 → 0
    older = body["rows"][1]
    assert older["stock_daily"] == 3
    assert older["stock_minute"] == 0  # 该日无分钟数据


def test_pagination(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats?page=2&page_size=1").json()
    assert body["total"] == 2
    assert [row["date"] for row in body["rows"]] == ["2026-08-14"]
    empty = client.get("/api/data/local-market-stats?page=99&page_size=15").json()
    assert empty["total"] == 2
    assert empty["rows"] == []


def test_page_size_validation(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    assert client.get("/api/data/local-market-stats?page=0").status_code == 422
    assert client.get("/api/data/local-market-stats?page_size=101").status_code == 422


def test_empty_data_dir(tmp_path: Path) -> None:
    repo = _FakeRepo(tmp_path / "data" / "nope")
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats").json()
    assert body == {"total": 0, "page": 1, "page_size": 15, "rows": []}


def test_check_day_endpoint_triggers(monkeypatch) -> None:
    from app.quant.datasource import network_client as nc
    calls: list[tuple] = []

    class _FakeClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            calls.append((kind, params))

    monkeypatch.setattr(nc, "StockDataClient", _FakeClient)
    client = TestClient(_make_app(repo))
    r = client.post("/api/data/check-day", json={"date": "2026-08-05"})
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    assert calls == [("check_day", {"day": "2026-08-05"})]
    bad = client.post("/api/data/check-day", json={"date": "not-a-date"})
    assert bad.status_code == 400


def test_check_full_endpoint_triggers(monkeypatch) -> None:
    from app.quant.datasource import network_client as nc
    calls: list[tuple] = []

    class _FakeClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            calls.append((kind, params))

    monkeypatch.setattr(nc, "StockDataClient", _FakeClient)
    client = TestClient(_make_app(repo))
    r = client.post("/api/data/check-full")
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    assert calls == [("check_full", {})]


def test_check_day_endpoint_503_when_service_down(monkeypatch) -> None:
    from app.quant.datasource import network_client as nc

    class _BrokenClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            raise ConnectionError("stockdata down")

    monkeypatch.setattr(nc, "StockDataClient", _BrokenClient)
    client = TestClient(_make_app(repo))
    r = client.post("/api/data/check-day", json={"date": "2026-08-05"})
    assert r.status_code == 503