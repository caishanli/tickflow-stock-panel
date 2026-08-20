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
    app.state.capabilities = SimpleNamespace(has=lambda *_: True)
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


def test_check_day_endpoint_triggers(monkeypatch, repo: _FakeRepo) -> None:
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


def test_check_full_endpoint_triggers(monkeypatch, repo: _FakeRepo) -> None:
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


def test_complement_daily_bj_filters_bj_symbols(monkeypatch, tmp_path: Path) -> None:
    """补 BJ 只拉 920xxx.BJ 标的, 透传日期区间给批量同步。"""
    from datetime import datetime as _dt

    from app.jobs import daily_pipeline
    from app.services import kline_sync

    called: dict = {}

    def fake_universe(capset) -> list[str]:
        return ["000001.SZ", "920425.BJ", "600000.SH", "920035.BJ", "000002.SZ"]

    def fake_sync(symbols, repo, capset, **kw):
        called["symbols"] = list(symbols)
        called.update(kw)
        return 7

    monkeypatch.setattr(daily_pipeline, "_resolve_universe", fake_universe)
    monkeypatch.setattr(kline_sync, "sync_and_persist_daily_batch", fake_sync)
    repo = _FakeRepo(tmp_path / "data")
    n = kline_sync.complement_daily_bj(
        repo, None,
        start_date=_dt(2026, 8, 7), end_date=_dt(2026, 8, 8),
    )
    assert n == 7
    assert called["symbols"] == ["920035.BJ", "920425.BJ"]
    assert called["start_date"] == _dt(2026, 8, 7)
    assert called["end_date"] == _dt(2026, 8, 8)


def test_complement_daily_bj_empty_universe(monkeypatch, tmp_path: Path) -> None:
    """宇宙无 BJ 标的 → 跳过, 不触发批量同步。"""
    from app.jobs import daily_pipeline
    from app.services import kline_sync

    monkeypatch.setattr(daily_pipeline, "_resolve_universe", lambda capset: ["000001.SZ"])
    called = []

    def fake_sync(symbols, repo, capset, **kw):
        called.append(symbols)
        return 0

    monkeypatch.setattr(kline_sync, "sync_and_persist_daily_batch", fake_sync)
    repo = _FakeRepo(tmp_path / "data")
    assert kline_sync.complement_daily_bj(repo, None) == 0
    assert called == []


def test_check_day_endpoint_spawns_bj_complement(monkeypatch, repo: _FakeRepo) -> None:
    """check-day 触发 stockdata 后, 叠加后台补 BJ 线程(Thread.start 同步执行 target)。"""
    from datetime import datetime as _dt

    from app.quant.datasource import network_client as nc
    from app.services import kline_sync

    trigger_calls: list[tuple] = []
    bj_calls: list[tuple] = []

    class _FakeClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            trigger_calls.append((kind, params))

    class _SyncThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None) -> None:
            self.target = target
            self.args = args
            self.kwargs = kwargs or {}

        def start(self) -> None:
            self.target(*self.args, **self.kwargs)

    monkeypatch.setattr(nc, "StockDataClient", _FakeClient)
    monkeypatch.setattr(api.threading, "Thread", _SyncThread)
    monkeypatch.setattr(
        kline_sync, "complement_daily_bj",
        lambda *a, **k: bj_calls.append((a, k)) or 0,
    )

    app = _make_app(repo)
    client = TestClient(app)
    r = client.post("/api/data/check-day", json={"date": "2026-08-07"})
    assert r.status_code == 200
    assert trigger_calls == [("check_day", {"day": "2026-08-07"})]
    assert bj_calls, "check-day 应叠加补 BJ 线程"
    _, kwargs = bj_calls[0]
    assert kwargs["start_date"] == _dt(2026, 8, 7)
    assert kwargs["end_date"] == _dt(2026, 8, 8)


def test_check_full_endpoint_spawns_bj_complement(monkeypatch, repo: _FakeRepo) -> None:
    """check-full 触发 stockdata 后, 叠加覆盖本地全部日K日期的后台补 BJ 线程。"""
    from datetime import datetime as _dt

    from app.quant.datasource import network_client as nc
    from app.services import kline_sync

    trigger_calls: list[tuple] = []
    bj_calls: list[tuple] = []

    class _FakeClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            trigger_calls.append((kind, params))

    class _SyncThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None) -> None:
            self.target = target
            self.args = args
            self.kwargs = kwargs or {}

        def start(self) -> None:
            self.target(*self.args, **self.kwargs)

    monkeypatch.setattr(nc, "StockDataClient", _FakeClient)
    monkeypatch.setattr(api.threading, "Thread", _SyncThread)
    monkeypatch.setattr(
        kline_sync, "complement_daily_bj",
        lambda *a, **k: bj_calls.append((a, k)) or 0,
    )

    client = TestClient(_make_app(repo))
    r = client.post("/api/data/check-full")
    assert r.status_code == 200
    assert trigger_calls == [("check_full", {})]
    assert bj_calls, "check-full 应叠加补 BJ 线程"
    _, kwargs = bj_calls[0]
    # 本地 kline_daily 日期为 2026-08-14 / 2026-08-17, start=最早(08-14) end=今天之后
    assert kwargs["start_date"] == _dt(2026, 8, 14)
    assert kwargs["end_date"] > _dt(2026, 8, 17)


def test_date_range_filter_start(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats?start_date=2026-08-16").json()
    assert [r["date"] for r in body["rows"]] == ["2026-08-17"]
    assert body["total"] == 1


def test_date_range_filter_end(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats?end_date=2026-08-15").json()
    assert [r["date"] for r in body["rows"]] == ["2026-08-14"]
    assert body["total"] == 1


def test_date_range_filter_both_inclusive(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get(
        "/api/data/local-market-stats?start_date=2026-08-14&end_date=2026-08-17"
    ).json()
    assert [r["date"] for r in body["rows"]] == ["2026-08-17", "2026-08-14"]
    assert body["total"] == 2


def test_date_range_filter_no_match(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get(
        "/api/data/local-market-stats?start_date=2026-09-01&end_date=2026-09-30"
    ).json()
    assert body["total"] == 0
    assert body["rows"] == []


def test_date_range_filter_invalid_date(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    assert client.get("/api/data/local-market-stats?start_date=not-a-date").status_code == 400


def test_local_market_stats_refresh_bypasses_cache(repo: _FakeRepo, tmp_path: Path) -> None:
    # 复用默认 repo 首次请求 → 命中缓存
    client = TestClient(_make_app(repo))
    first = client.get("/api/data/local-market-stats?page=1&page_size=15").json()
    assert first["rows"][0]["stock_daily"] == 2

    # 磁盘改动: 往 2026-08-17 分区加一个新 symbol
    part = repo.store.data_dir / "kline_daily" / "date=2026-08-17"
    df = pl.DataFrame({
        "symbol": ["000001.SZ", "600000.SH", "000002.SZ"],
        "date": [date(2026, 8, 17)] * 3,
    })
    df.write_parquet(part / "part.parquet")

    # 普通请求仍返回旧缓存 (2)
    cached = client.get("/api/data/local-market-stats?page=1&page_size=15").json()
    assert cached["rows"][0]["stock_daily"] == 2

    # refresh=1 绕过缓存 → 新值 (3)
    refreshed = client.get("/api/data/local-market-stats?page=1&page_size=15&refresh=1").json()
    assert refreshed["rows"][0]["stock_daily"] == 3
