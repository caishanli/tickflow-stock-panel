# backend/tests/quant/test_stockdata_sources.py
import datetime as _dt

import polars as pl
import pytest

from app.services.stockdata.sources import (
    DataSources,
    MinuteMemoryStore,
    NetworkPuller,
    _is_index,
    _pull_recent_guarded,
)


def _write_daily(root, day, rows):
    import os
    d = os.path.join(root, "kline_daily", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame(rows).write_parquet(os.path.join(d, "part.parquet"))


def _write_minute(root, sub, day, rows):
    import os
    d = os.path.join(root, sub, f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame(rows).write_parquet(os.path.join(d, "part.parquet"))


@pytest.fixture
def src(tmp_path):
    import os
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    day = _dt.date.today().isoformat()
    _write_daily(str(tmp_path), day, [
        {"symbol": "600000.SH", "date": day, "open": 10.0, "high": 11.0,
         "low": 9.0, "close": 10.5, "volume": 1000, "amount": 105000.0},
    ])
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=2)
    yield s
    os.environ.pop("PARTITION_DATA_ROOT", None)


def test_preload_daily_reads_partitions(src):
    df = src.preload_daily(lookback_days=400)
    assert not df.is_empty()
    assert "symbol" in df.columns and "close" in df.columns
    assert df["symbol"].to_list() == ["600000.SH"]
    # 股票日线 volume 手 → 股（×100）
    assert df["volume"].to_list() == [100000]


def test_preload_daily_excludes_index_but_get_daily_serves(tmp_path):
    """预载批量不含指数（防 932xxx 等指数代码污染 ETF 宇宙），
    但 get_daily 按需仍服务指数（策略 get_price 指数走这条）。"""
    import os
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    day = _dt.date.today().isoformat()
    etf = {"symbol": "512670.SH", "date": day, "open": 1.0, "high": 1.0,
           "low": 1.0, "close": 1.0, "volume": 10000, "amount": 10000.0}
    idx = {"symbol": "932000.SH", "date": day, "open": 3000.0, "high": 3000.0,
           "low": 3000.0, "close": 3000.0, "volume": 100.0, "amount": 300000.0}
    d = os.path.join(str(tmp_path), "kline_etf_daily", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame([etf]).write_parquet(os.path.join(d, "part.parquet"))
    d2 = os.path.join(str(tmp_path), "kline_index_daily", f"date={day}")
    os.makedirs(d2, exist_ok=True)
    pl.DataFrame([idx]).write_parquet(os.path.join(d2, "part.parquet"))
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=2)
    try:
        pre = s.preload_daily(lookback_days=400)
        assert "932000.SH" not in pre["symbol"].to_list()
        assert "512670.SH" in pre["symbol"].to_list()
        got = s.get_daily(["932000.XSHG"], day, day)
        assert not got.is_empty()
        assert got["symbol"].to_list() == ["932000.SH"]
    finally:
        os.environ.pop("PARTITION_DATA_ROOT", None)


def test_minute_memory_store_lazy_and_clear():
    ms = MinuteMemoryStore()
    day = _dt.date.today()
    assert ms.day() is None  # 未请求标的：无日期、无内存
    df = pl.DataFrame({"symbol": ["600000.SH"], "datetime": [f"{day} 10:00:00"],
                       "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
                       "volume": [100], "amount": [100.0]})
    ms.update(f"{day} 00:00:00", df)
    assert ms.day() == day
    got = ms.get_slice({"600000.SH"}, "2000-01-01 00:00:00", f"{day} 15:00:00")
    assert not got.is_empty()
    # 换日 lazy 清空：ensure_day(次日) 后旧帧全部清空
    nxt = day + _dt.timedelta(days=1)
    ms.ensure_day(nxt)
    assert ms.day() == nxt
    assert ms.get_slice({"600000.SH"}, "2000-01-01 00:00:00", f"{day} 15:00:00").is_empty()
    # clear() 显式清空后回到初始态
    ms.clear()
    assert ms.day() is None


def test_realtime_snapshot_serves_from_memory(src, monkeypatch):
    day = _dt.date.today().isoformat()
    _write_minute(str(src.data_root), "kline_etf_minute", day, [
        {"symbol": "512670.SH", "datetime": f"{day} 09:31:00", "open": 1.0,
         "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1000, "amount": 1000.0},
    ])
    # 非交易时段：只读内存库，不触网
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    df = src.get_realtime_snapshot(["512670.XSHG"])
    assert not df.is_empty()
    assert df["close"].to_list() == [1.0]


def test_realtime_snapshot_empty_no_crash(src, monkeypatch):
    """标的当日无分区、非交易时段不触网：返回空帧而非 Utf8 与 datetime 比较崩溃。"""
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    df = src.get_realtime_snapshot(["600000.XSHG"])
    assert df.is_empty()
    assert list(df.columns) == ["symbol", "datetime", "open", "high", "low",
                                "close", "volume", "amount"]


def test_realtime_snapshot_empty_in_trading_no_crash(tmp_path, monkeypatch):
    """交易时段但当日无分区/内存（回源也拿不到数据）：仍返回空帧、不崩溃。"""
    class StubSrc:
        def get_minute_recent(self, code, pages=1):
            import pandas as pd
            return pd.DataFrame()

    s = DataSources(data_root=str(tmp_path), mootdx_factory=lambda: StubSrc(), fetch_workers=1)
    try:
        monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: True)
        df = s.get_realtime_snapshot(["600000.XSHG"])
        assert df.is_empty()
    finally:
        s.puller.shutdown()


def test_is_index_suffix_based():
    assert _is_index("000001.XSHE") is False   # 深市 000001 平安银行是股票
    assert _is_index("000157.XSHE") is False   # 深市 000157 中联重科是股票
    assert _is_index("000300.XSHG") is True    # 沪市 000xxx 是指数
    assert _is_index("399006.XSHE") is True    # 399 深证指数，任意市场
    assert _is_index("512670.XSHG") is False   # ETF 不是指数


def test_pull_recent_guarded_timeout_raises():
    class HangSrc:
        def get_minute_recent(self, code, pages=1):
            import time
            time.sleep(60)

    with pytest.raises(TimeoutError):
        _pull_recent_guarded(HangSrc(), "600000.XSHG", timeout=0.05)


def test_fetch_one_rebuilds_source_on_timeout(monkeypatch):
    """超时后线程本地数据源被重置：下次 fetch 重建（factory 调用次数递增）。"""
    calls = []

    def fake_factory():
        calls.append(object())
        return calls[-1]

    def fake_pull(src, code):
        raise TimeoutError(f"timeout {code}")

    monkeypatch.setattr("app.services.stockdata.sources._pull_recent_guarded", fake_pull)
    p = NetworkPuller(factory=fake_factory, workers=1)
    try:
        assert p._fetch_one("600000.XSHG").is_empty()
        assert len(calls) == 1
        assert getattr(p._local, "src", None) is None  # 已重置，不复用坏 socket
        assert p._fetch_one("600000.XSHG").is_empty()
        assert len(calls) == 2  # 第二次 fetch 重建数据源
    finally:
        p.shutdown()


def test_metadata_methods_with_ohlcv_only_partitions(src):
    """分区仅 symbol/OHLCV：元数据方法不再因缺 name/list_date 列而崩。"""
    df = src.get_all_securities(["stock"], None)
    assert not df.is_empty()
    assert list(df.columns) == ["symbol", "type", "name", "list_date"]
    row = df.to_dicts()[0]
    assert row["symbol"] == "600000.SH"
    assert row["type"] == "stock"
    assert row["name"] is None and row["list_date"] is None

    info = src.get_security_info("600000.XSHG")
    assert info["code"] == "600000.XSHG"
    assert info["type"] == "stock"
    assert info["name"] is None and info["start_date"] is None
    assert info["end_date"] is None

    assert src.get_stock_names() == {}
