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


def test_get_daily_accepts_compact_date_format(tmp_path):
    """get_daily 必须兼容 %Y%m%d 无横线日期（模拟盘 jqcompat 传此格式）。

    回归：服务端 _scan_partitions 用字符串比较分区名（ISO 带横线），
    '20260601' 与 '2026-06-01' 比较恒 False → 全部分区被跳过返回空，
    导致指数走弱期判断「数据不足」、全球池成交额过滤静默失效。
    """
    import os
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    day = "2026-07-10"
    idx = {"symbol": "000300.SH", "date": day, "open": 3000.0, "high": 3000.0,
           "low": 3000.0, "close": 3000.0, "volume": 100.0, "amount": 300000.0}
    d = os.path.join(str(tmp_path), "kline_index_daily", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame([idx]).write_parquet(os.path.join(d, "part.parquet"))
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=2)
    try:
        # 无横线 %Y%m%d 格式（jqcompat _DayBarStore.get_bars 传入）
        got = s.get_daily(["000300.XSHG"], "20260601", "20260710")
        assert not got.is_empty()
        assert got["symbol"].to_list() == ["000300.SH"]
        # 带横线 ISO 格式（rqalpha_bridge 传入）不受影响
        got2 = s.get_daily(["000300.XSHG"], "2026-06-01", "2026-07-10")
        assert not got2.is_empty()
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


def test_realtime_snapshot_mixed_ns_us_datetime(src, monkeypatch):
    """回归：分区帧 datetime 为 Datetime('us')（parquet 落盘口径），实时回源
    pl.from_pandas 为 Datetime('ns')；两者经 _as_datetime 应统一单位，pl.concat
    不再抛 Datetime('ns') vs Datetime('μs') SchemaError。"""
    import pandas as pd

    day = _dt.date.today().isoformat()
    _write_minute(str(src.data_root), "kline_etf_minute", day, [
        {"symbol": "512670.SH", "datetime": f"{day} 09:31:00", "open": 1.0,
         "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1000, "amount": 1000.0},
    ])
    # 模拟实时回源帧：pandas 默认 Datetime('ns')
    pdf = pd.DataFrame({
        "symbol": ["512670.SH"], "datetime": pd.to_datetime([f"{day} 10:00:00"]),
        "open": [1.1], "high": [1.1], "low": [1.1], "close": [1.1],
        "volume": [2000], "amount": [2000.0],
    })
    ns_df = pl.from_pandas(pdf)
    assert ns_df.schema["datetime"] == pl.Datetime("ns")
    src.minute_store.update(day, ns_df)
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    df = src.get_realtime_snapshot(["512670.XSHG"])
    assert not df.is_empty()
    # 分区 09:31 与内存 10:00 两条都应在（unique keep="last" 只去同秒重复）
    assert len(df) == 2


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
