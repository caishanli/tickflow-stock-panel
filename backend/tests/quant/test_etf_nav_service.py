"""etf_nav_service：akshare 全市场净值 → 按日分区落盘 + 缺失日巡检。"""
import datetime as _dt
from datetime import date

import polars as pl
import pytest

from app.services import etf_nav_service as svc
from app.services import mootdx_service


def test_sync_writes_partition_and_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)

    def fake_fund_etf_fund_daily_em():
        return pl.DataFrame({
            "基金代码": ["510300", "159915", "518880"],
            "单位净值": [4.7556, 3.5860, 5.0123],
        })

    monkeypatch.setattr(svc, "_fund_etf_fund_daily_em", fake_fund_etf_fund_daily_em)
    day = date(2026, 8, 7)
    n1 = svc.sync_etf_nav(day)
    n2 = svc.sync_etf_nav(day)  # 幂等：分区已存在 → 跳过
    assert n1 == 3
    assert n2 == 0
    part = tmp_path / "etf_nav" / "date=2026-08-07" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    assert sorted(df["symbol"].to_list()) == ["159915.XSHE", "510300.XSHG", "518880.XSHG"]
    assert df["unit_nav"].dtype == pl.Float64
    assert set(df["date"].to_list()) == {"2026-08-07"}


def test_symbol_market_mapping():
    # 5/6/9 开头 → 沪市 XSHG；0/1/2/3 开头 → 深市 XSHE
    assert svc._jq_symbol("510300") == "510300.XSHG"
    assert svc._jq_symbol("159915") == "159915.XSHE"
    assert svc._jq_symbol("518880") == "518880.XSHG"


def test_missing_nav_days_respects_market_close(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    # 完全空分区：收盘前不算缺失（当日无当日净值），收盘后算今天
    assert svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 14, 0)) == []
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30))
    assert _dt.date(2026, 8, 7) in missing
    # 已有当日分区 → 不再缺失
    (tmp_path / "etf_nav" / "date=2026-08-07").mkdir(parents=True)
    assert _dt.date(2026, 8, 7) not in svc._missing_etf_nav_days(
        now=_dt.datetime(2026, 8, 7, 15, 30))


def test_scan_missing_partitions_has_etf_nav(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    importlib.reload(mootdx_service)
    missing = mootdx_service.scan_missing_partitions()
    assert "etf_nav" in missing
    assert isinstance(missing["etf_nav"], list)
