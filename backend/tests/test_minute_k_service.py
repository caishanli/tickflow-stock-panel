"""Tests for MinuteKService."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from app.services.minute_k_service import MinuteKService, MinuteKWorker, _discover_servers
from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    r = KlineRepository(store=store)
    yield r
    store.db.close()


@pytest.fixture
def service(repo: KlineRepository) -> MinuteKService:
    return MinuteKService(repo, interval=1.0)


class TestMinuteKService:
    def test_init(self, service: MinuteKService) -> None:
        assert service._interval == 1.0
        assert not service._running
        assert not service._enabled

    def test_status_when_stopped(self, service: MinuteKService) -> None:
        s = service.status()
        assert s["enabled"] is False
        assert s["running"] is False
        assert s["interval_s"] == 1.0

    def test_pause_resume(self, service: MinuteKService) -> None:
        service.pause()
        assert service._paused is True
        service.resume()
        assert service._paused is False

    def test_write_to_duckdb(self, service: MinuteKService) -> None:
        df = pl.DataFrame({
            "symbol": ["510300.SH", "510300.SH"],
            "datetime": [datetime(2026, 7, 30, 9, 30), datetime(2026, 7, 30, 9, 31)],
            "open": [4.0, 4.1],
            "high": [4.1, 4.2],
            "low": [3.9, 4.0],
            "close": [4.05, 4.15],
            "volume": [1000.0, 1200.0],
            "amount": [4050.0, 5000.0],
        })
        service._write_to_duckdb(df)
        result = service._repo.db.execute(
            "SELECT * FROM kline_minute WHERE symbol = '510300.SH'"
        ).fetchall()
        assert len(result) == 2

    def test_write_empty_df(self, service: MinuteKService) -> None:
        service._write_to_duckdb(pl.DataFrame())


class TestMinuteKWorker:
    def test_worker_init(self) -> None:
        results = []
        errors = []
        w = MinuteKWorker(
            server=("1.2.3.4", 7709),
            symbols=["510300.SH"],
            results=results,
            errors=errors,
        )
        assert w._server == ("1.2.3.4", 7709)
        assert w._symbols == ["510300.SH"]
        assert w.daemon is True


class TestDiscoverServers:
    @patch("app.services.minute_k_service._probe")
    def test_discover_returns_reachable(self, mock_probe: MagicMock) -> None:
        mock_probe.side_effect = lambda ip, port, **kw: ip == "1.2.3.4"
        servers = _discover_servers(max_workers=5)
        assert all(s[0] == "1.2.3.4" for s in servers)

    @patch("app.services.minute_k_service._probe", return_value=False)
    def test_discover_returns_empty_when_none_reachable(self, _: MagicMock) -> None:
        servers = _discover_servers()
        assert servers == []
