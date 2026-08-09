"""etf_nav_service：akshare 全市场净值 → 按日分区落盘 + 缺失日巡检。"""
import datetime as _dt
from datetime import date

import polars as pl

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
    assert svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30)) == []


def test_missing_nav_days_only_syncs_latest_not_history(tmp_path, monkeypatch):
    """历史不补：只有最新缺失交易日会被回源，不会写假历史分区。

    akshare fund_etf_fund_daily_em 只有当前快照（无逐日历史），回补历史只会
    把今日净值写成 4/1 以来的每一天。因此 _missing_etf_nav_days 最多返回
    **一个**候选日（最新分区之后最近一个已收盘交易日）。
    """
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    # 已有 08-05 分区，今天是 08-07 收盘后 → 只缺 08-07，不返回 08-06 之前的历史
    (tmp_path / "etf_nav" / "date=2026-08-05").mkdir(parents=True)
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30))
    assert missing == [_dt.date(2026, 8, 7)]
    assert len(missing) <= 1  # 历史不补的核心断言


def test_missing_nav_days_empty_root_returns_single_latest(tmp_path, monkeypatch):
    """冷启动空分区：只返回最近一个交易日，不铺开 4/1 以来的全部历史。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30))
    assert len(missing) <= 1


def test_scan_missing_partitions_has_etf_nav(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    importlib.reload(mootdx_service)
    missing = mootdx_service.scan_missing_partitions()
    assert "etf_nav" in missing
    assert isinstance(missing["etf_nav"], list)
    assert len(missing["etf_nav"]) <= 1


def test_backfill_to_now_includes_etf_nav(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    importlib.reload(mootdx_service)

    def fake_sync(day=None):
        return 3

    def fake_missing(now=None):
        return [_dt.date(2026, 8, 7)]

    monkeypatch.setattr(svc, "sync_etf_nav", fake_sync)
    monkeypatch.setattr(svc, "_missing_etf_nav_days", fake_missing)
    # 避免触发真实网络回源（mootdx 可达时 backfill_to_now 会真的回源几十个
    # 交易日）。对齐 test_mootdx_backfill_coverage 的既有 backfill 测试口径，
    # 把所有网络 sync 都 mock 成 no-op，只验证 etf_nav 接线。
    monkeypatch.setattr(mootdx_service, "_missing_minute_days", lambda now=None: [])
    monkeypatch.setattr(mootdx_service, "_missing_daily_days", lambda root, now=None: [])
    monkeypatch.setattr(mootdx_service, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(mootdx_service, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(mootdx_service, "_adj_factor_stale", lambda: False)
    monkeypatch.setattr(mootdx_service, "sync_etf_minute", lambda day=None: 0)
    monkeypatch.setattr(mootdx_service, "sync_daily", lambda day: {"stock": 0, "etf": 0})
    monkeypatch.setattr(mootdx_service, "sync_index_daily", lambda day: {"written": 0})
    monkeypatch.setattr(mootdx_service, "sync_adj_factor", lambda: None)
    monkeypatch.setattr(mootdx_service, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(mootdx_service, "_notify_missing", lambda m: None)

    res = mootdx_service.backfill_to_now()
    assert "etf_nav_days" in res
    assert "etf_nav" in res.get("missing", {})
