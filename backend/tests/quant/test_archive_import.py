import io
import zipfile

import polars as pl

from app.quant.archive_import import (
    convert_daily,
    convert_minute,
    day_to_partition,
    iter_zip_days,
    read_day_from_zip,
)


def _archive_minute_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "code": ["000001.SZ", "000001.SZ", "600000.SH"],
            "trade_time": ["2019-01-02 09:30:00", "2019-01-02 09:31:00", "2019-01-02 09:30:00"],
            "open": [9.39, 9.39, 10.0],
            "high": [9.40, 9.42, 10.1],
            "low": [9.38, 9.37, 9.9],
            "close": [9.39, 9.40, 10.05],
            "vol": [389500.0, 831797.0, 1000.0],
            "amount": [3657000.0, 7817087.0, 10050.0],
            "date": ["20190102"] * 3,
            "pre_close": [9.38, 9.38, 9.9],
            "change": [0.01, 0.02, 0.15],
            "pct_chg": [0.11, 0.21, 1.5],
            "__index_level_0__": [0, 1, 0],
        }
    )


def _archive_daily_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "code": ["000001.SZ", "600000.SH"],
            "date": ["20190102", "20190102"],
            "open": [9.24, 10.0],
            "high": [9.82, 10.1],
            "low": [9.22, 9.9],
            "close": [9.75, 10.05],
            "pre_close": [9.28, 9.9],
            "change": [0.47, 0.15],
            "pct_chg": [5.06, 1.5],
            "vol": [84038656.0, 100000.0],
            "amount": [980347617.0, 1005000.0],
            "adj_factor": [108.031, 100.0],
            "vwap": [9.2, 10.05],
        }
    )


def test_convert_minute_matches_local_schema():
    """归档 1m → 本机 kline_minute 的 8 列 schema。"""
    out = convert_minute(_archive_minute_frame())
    assert out.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert out.schema["symbol"] == pl.String
    assert out.schema["datetime"] == pl.Datetime("us")
    for c in ("open", "high", "low", "close", "volume", "amount"):
        assert out.schema[c] == pl.Float64


def test_convert_minute_keeps_volume_in_shares():
    """归档 1m 的 vol 单位是「股」，与本机 kline_minute 一致 → 不换算。"""
    out = convert_minute(_archive_minute_frame())
    assert out["volume"].to_list() == [389500.0, 831797.0, 1000.0]


def test_convert_daily_divides_volume_by_100():
    """归档 1d 的 vol 是「股」，本机 kline_daily 存「手」→ ÷100。"""
    out = convert_daily(_archive_daily_frame())
    assert out["volume"].to_list() == [840386.56, 1000.0]
    assert out.schema["date"] == pl.Date


def test_convert_daily_drops_archive_only_columns():
    """归档多出的 pre_close/change/pct_chg/adj_factor/vwap 不能进本机分区。"""
    out = convert_daily(_archive_daily_frame())
    assert out.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]


def test_day_to_partition():
    assert day_to_partition("20190102.parquet") == "2019-01-02"


def test_iter_and_read_from_zip(tmp_path):
    buf1 = io.BytesIO()
    _archive_minute_frame().write_parquet(buf1)
    buf2 = io.BytesIO()
    _archive_minute_frame().write_parquet(buf2)
    zpath = tmp_path / "2019.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("20190103.parquet", buf1.getvalue())
        zf.writestr("20190102.parquet", buf2.getvalue())
        zf.writestr("nested/20190104.parquet", buf1.getvalue())
    days = iter_zip_days(zpath)
    assert days == ["20190102.parquet", "20190103.parquet"]   # 排除嵌套目录
    df = read_day_from_zip(zpath, "20190102.parquet")
    assert df.height == 3
