from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from app.services import instrument_sync as sync


def _item(ex, code, name="新名称"):
    return {"symbol": f"{code}.{ex}", "code": code, "name": name,
            "exchange": ex, "type": "stock", "ext": {"total_shares": 123}}


def _install(monkeypatch, failed="SZ", empty=False):
    monkeypatch.setattr(sync, "_fetch_instruments_via_provider", lambda: None)

    def fetch(ex, **kwargs):
        if ex == failed:
            if empty:
                return []
            raise RuntimeError("503 Service Unavailable")
        return [_item(ex, "600000" if ex == "SH" else "920001")]

    monkeypatch.setattr(sync, "get_client", lambda: SimpleNamespace(
        exchanges=SimpleNamespace(get_instruments=fetch)))


def _seed(tmp_path):
    path = tmp_path / "instruments" / "instruments.parquet"
    path.parent.mkdir()
    rows = sync._flatten_instruments([
        _item("SH", "600000", "旧沪市"), _item("SZ", "000001", "旧深市")])
    pl.DataFrame(rows).with_columns(pl.lit(date(2026, 10, 9)).alias("as_of")).write_parquet(path)
    return path


@pytest.mark.parametrize("empty", [False, True])
def test_failed_exchange_preserves_cached_rows_and_snapshot_date(monkeypatch, tmp_path, empty):
    _install(monkeypatch, empty=empty)
    path = _seed(tmp_path)
    assert sync.sync_instruments(tmp_path) == 3
    df = pl.read_parquet(path)
    assert set(df["symbol"]) == {"600000.SH", "000001.SZ", "920001.BJ"}
    old = df.filter(pl.col("exchange") == "SZ").row(0, named=True)
    assert old["name"] == "旧深市" and old["as_of"] == date(2026, 10, 9)
    assert df.filter(pl.col("exchange") == "SH")["name"][0] == "新名称"


def test_failed_exchange_without_cache_does_not_publish_partial_market(monkeypatch, tmp_path):
    _install(monkeypatch)
    path = _seed(tmp_path)
    pl.read_parquet(path).filter(pl.col("exchange") == "SH").write_parquet(path)
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="SZ"):
        sync.sync_instruments(tmp_path)
    assert path.read_bytes() == before


def test_successful_sync_replaces_old_market(monkeypatch, tmp_path):
    _install(monkeypatch, failed=None)
    path = _seed(tmp_path)
    assert sync.sync_instruments(tmp_path) == 3
    assert set(pl.read_parquet(path)["symbol"]) == {"600000.SH", "920001.SZ", "920001.BJ"}


def test_failed_atomic_write_preserves_existing_snapshot(monkeypatch, tmp_path):
    _install(monkeypatch, failed=None)
    path = _seed(tmp_path)
    before = path.read_bytes()

    def fail(*args):
        raise OSError("publish failed")

    monkeypatch.setattr(sync.os, "replace", fail)
    with pytest.raises(OSError, match="publish failed"):
        sync.sync_instruments(tmp_path)
    assert path.read_bytes() == before
    assert not list(path.parent.glob("*.tmp"))


@pytest.mark.parametrize("empty", [False, True])
def test_pre_market_requires_all_exchanges_fresh_before_success(monkeypatch, tmp_path, empty):
    _install(monkeypatch, empty=empty)
    path = _seed(tmp_path)
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match=r"SZ.*下一轮重试"):
        sync.sync_instruments(tmp_path, require_complete=True)
    assert path.read_bytes() == before


def test_pre_market_empty_provider_is_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(sync, "_fetch_instruments_via_provider", lambda: [])
    with pytest.raises(RuntimeError, match="空名录"):
        sync.sync_instruments(tmp_path, require_complete=True)
