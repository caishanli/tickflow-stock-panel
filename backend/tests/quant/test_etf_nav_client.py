"""读取侧：DataSources.get_etf_nav + StockDataClient 往返 + get_extras 真实净值。"""
import pandas as pd
import polars as pl

from app.quant.jqengine.engine.jq import api as sim_api
from app.services.stockdata.sources import DataSources


def _write_nav_partition(root, day, rows):
    pdir = root / "etf_nav" / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows)
    df.write_parquet(pdir / "part.parquet")


def test_datasources_get_etf_nav(tmp_path):
    _write_nav_partition(tmp_path, "2026-08-07", {
        "symbol": ["510300.XSHG", "159915.XSHE"],
        "unit_nav": [4.7556, 3.5860],
        "date": ["2026-08-07", "2026-08-07"],
    })
    ds = DataSources(data_root=str(tmp_path))
    df = ds.get_etf_nav(["510300.XSHG"], "2026-08-07")
    assert df is not None and df.height == 1
    row = df.filter(pl.col("symbol") == "510300.XSHG").to_dicts()[0]
    assert abs(row["unit_nav"] - 4.7556) < 1e-9


def test_datasources_get_etf_nav_latest_partition(tmp_path):
    _write_nav_partition(tmp_path, "2026-08-06", {
        "symbol": ["510300.XSHG"], "unit_nav": [4.7111],
        "date": ["2026-08-06"],
    })
    ds = DataSources(data_root=str(tmp_path))
    df = ds.get_etf_nav(["510300.XSHG"], None)  # 无 date → 最新分区
    assert df is not None and df.height == 1
    assert df["date"].to_list() == ["2026-08-06"]


class _FakeNavClient:
    def __init__(self, navs):
        self.navs = navs  # {code: {date_str: unit_nav}}

    def get_etf_nav(self, codes, date=None):
        out = {}
        for c in codes:
            rows = self.navs.get(c, {})
            idx = sorted(rows)
            if date is not None:
                idx = [d for d in idx if d <= date]
            out[c] = pd.DataFrame({
                "date": idx,
                "unit_nav": [rows[d] for d in idx],
            })
        return out


class _FakeNavManager:
    def __init__(self, navs):
        self.client = _FakeNavClient(navs)


def test_sim_get_extras_returns_real_nav(monkeypatch):
    navs = {"510300.XSHG": {"2026-08-07": 4.7556}}
    mgr = _FakeNavManager(navs)
    sim_api._reset(mgr, 0.0003, 0.001, 10000.0)
    df = sim_api.get_extras("unit_net_value", ["510300.XSHG"],
                            "2026-08-07", "2026-08-07")
    assert not df.empty
    assert list(df.columns) == ["510300.XSHG"]
    assert abs(df["510300.XSHG"].iloc[-1] - 4.7556) < 1e-9


def test_sim_get_extras_missing_nav_returns_empty(monkeypatch):
    mgr = _FakeNavManager({})
    sim_api._reset(mgr, 0.0003, 0.001, 10000.0)
    df = sim_api.get_extras("unit_net_value", ["510300.XSHG"],
                            "2026-08-07", "2026-08-07")
    assert df.empty


class _RealShapeNavClient:
    """模拟真实 StockDataClient._parquet_to_dict 输出：DatetimeIndex + unit_nav。"""

    def __init__(self, navs):
        self.navs = navs

    def get_etf_nav(self, codes, date=None):
        out = {}
        for c in codes:
            rows = self.navs.get(c, {})
            idx = sorted(k for k in rows if date is None or k <= date)
            if not idx:
                out[c] = pd.DataFrame(columns=["unit_nav"])
                continue
            out[c] = pd.DataFrame(
                {"unit_nav": [rows[k] for k in idx]},
                index=pd.to_datetime(idx))
        return out


def test_sim_get_extras_real_client_shape(monkeypatch):
    """真实客户端帧（无 date 列，DatetimeIndex）也能解出净值。"""
    navs = {"510300.XSHG": {"2026-08-06": 4.7111, "2026-08-07": 4.7556}}
    mgr = _FakeNavManager.__new__(_FakeNavManager)
    mgr.client = _RealShapeNavClient(navs)
    sim_api._reset(mgr, 0.0003, 0.001, 10000.0)
    df = sim_api.get_extras("unit_net_value", ["510300.XSHG"],
                            "2026-08-01", "2026-08-07")
    assert list(df.columns) == ["510300.XSHG"]
    assert len(df) == 2
    assert abs(df["510300.XSHG"].iloc[-1] - 4.7556) < 1e-9


def test_unsupported_field_still_empty():
    sim_api._reset(_FakeNavManager({}), 0.0003, 0.001, 10000.0)
    df = sim_api.get_extras("acc_net_value", ["510300.XSHG"])
    assert df.empty
