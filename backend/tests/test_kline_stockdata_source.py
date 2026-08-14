"""kline API data_source=stockdata 的 helper 单测(不依赖 stockdata 服务进程)。"""
from datetime import date

import pandas as pd
import polars as pl

from app.api.kline import (
    _stockdata_daily,
    _stockdata_frame,
    _stockdata_is_etf,
    _stockdata_minute,
    _to_jq_code,
    _to_partition_symbol,
)


def test_to_jq_code():
    assert _to_jq_code("000001.SZ") == "000001.XSHE"
    assert _to_jq_code("600000.SH") == "600000.XSHG"
    assert _to_jq_code("600000.XSHG") == "600000.XSHG"
    assert _to_jq_code("920001.BJ") == "920001.XSHE"  # 未知后缀按深市


def test_stockdata_frame_restores_datetime_index():
    pdf = pd.DataFrame(
        {"open": [1.0, 2.0], "close": [1.1, 2.1]},
        index=pd.to_datetime(["2026-08-01 09:30:00", "2026-08-01 09:31:00"]),
    )
    out = {"600000.XSHG": pdf}
    df = _stockdata_frame(out, "600000.XSHG", "datetime")
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["datetime", "open", "close"]
    assert df["datetime"].cast(pl.Utf8).to_list()[0].startswith("2026-08-01")


def test_stockdata_minute_empty_on_client_error(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("service down")
    monkeypatch.setattr("app.api.kline._get_stockdata_client", boom)
    df = _stockdata_minute("000001.SZ", date(2026, 8, 1))
    assert df.is_empty()


def test_stockdata_daily_converts_stock_volume_to_lots(monkeypatch):
    """服务返回 volume 股 (x100), 股票转回手与 enriched 口径一致。"""
    pdf = pd.DataFrame(
        {"open": [1.0], "high": [1.1], "low": [0.9], "close": [1.05],
         "volume": [100000.0], "amount": [105000.0]},
        index=pd.to_datetime(["2026-08-01"]),
    )
    out = {"000001.XSHE": pdf}
    monkeypatch.setattr("app.api.kline._get_stockdata_client",
                        lambda: type("C", (), {"get_price": lambda self, *a, **k: out})())
    df = _stockdata_daily("000001.SZ", date(2026, 8, 1), date(2026, 8, 1), is_stock=True)
    assert not df.is_empty()
    assert df["volume"].to_list() == [1000.0]
    assert df["symbol"].to_list() == ["000001.SZ"]
    # date 列须为 pl.Date: 序列化后 str 与 enriched 路径一致 ("2026-08-01")
    assert str(df["date"].to_list()[0]) == "2026-08-01"


def test_stockdata_daily_empty_on_client_error(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("service down")
    monkeypatch.setattr("app.api.kline._get_stockdata_client", boom)
    df = _stockdata_daily("000001.SZ", date(2026, 8, 1), date(2026, 8, 1), is_stock=True)
    assert df.is_empty()


def test_to_partition_symbol():
    assert _to_partition_symbol("513360.XSHG") == "513360.SH"
    assert _to_partition_symbol("159227.XSHE") == "159227.SZ"
    assert _to_partition_symbol("513360.SH") == "513360.SH"
    assert _to_partition_symbol("920001.BJ") == "920001.SZ"


def test_stockdata_is_etf(monkeypatch):
    monkeypatch.setattr("app.api.kline._stockdata_etf_set", lambda: {"513360.SH", "159227.SZ"})
    assert _stockdata_is_etf("513360.XSHG")
    assert _stockdata_is_etf("159227.XSHE")
    assert not _stockdata_is_etf("000001.XSHE")


def test_stockdata_minute_converts_beijing_to_utc(monkeypatch):
    """服务返回北京时 naive datetime (09:31), 前端分时契约为 UTC naive → -8h 折算 (01:31)。"""
    import datetime as _dt
    pdf = pd.DataFrame(
        {"open": [1.0], "close": [1.1]},
        index=pd.to_datetime([_dt.datetime(2026, 8, 13, 9, 31)]),
    )
    out = {"000001.XSHE": pdf}
    monkeypatch.setattr("app.api.kline._get_stockdata_client",
                        lambda: type("C", (), {"get_minute_pool": lambda self, *a, **k: out})())
    df = _stockdata_minute("000001.XSHE", date(2026, 8, 13))
    assert not df.is_empty()
    assert df["datetime"].dt.strftime("%H:%M").to_list() == ["01:31"]
