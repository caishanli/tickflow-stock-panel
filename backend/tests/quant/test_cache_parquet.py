"""DataCache parquet 存储的回归测试（合成数据，不联网，tmp_path 隔离）。

覆盖 SQLite→parquet 迁移后的缓存语义：
- put→peek 往返：DatetimeIndex、数值、列完全等价
- df.attrs（adj/source 复权口径元数据）随 parquet 往返保留（pickle 等价语义），
  且 __attrs__ 元数据列不泄漏到读出的帧
- 空帧/None 不写盘；同键覆盖写 = 最后写入者胜，不留多个文件
- get：未命中走 loader 并落盘，命中不再调 loader
- get_all/keys/clear：批量读、键列表、清空；损坏文件在 get_all 中被跳过
"""
import os

import pandas as pd

from app.quant.jqengine.datasource.cache import DataCache

DATES = pd.date_range("2026-07-08 09:30", periods=3, freq="min")


def _minute_df():
    return pd.DataFrame({
        "open": [1.0, 1.1, 1.2], "high": [1.1, 1.2, 1.3],
        "low": [0.9, 1.0, 1.1], "close": [1.05, 1.15, 1.25],
        "volume": [100.0, 200.0, 300.0], "money": [105.0, 230.0, 375.0],
    }, index=DATES)


def test_put_peek_roundtrip(tmp_path):
    c = DataCache(root=str(tmp_path))
    df = _minute_df()
    c.put("minute", "real_510300.XSHG", df)
    out = c.peek("minute", "real_510300.XSHG")
    assert out is not None
    pd.testing.assert_frame_equal(out, df, check_freq=False)


def test_attrs_survive_roundtrip(tmp_path):
    c = DataCache(root=str(tmp_path))
    df = _minute_df()
    df.attrs["adj"] = "raw"
    df.attrs["source"] = "tushare"
    c.put("daily", "tushare_510300.XSHG", df)
    out = c.peek("daily", "tushare_510300.XSHG")
    assert out.attrs == {"adj": "raw", "source": "tushare"}
    assert "__attrs__" not in out.columns


def test_put_empty_or_none_skipped(tmp_path):
    c = DataCache(root=str(tmp_path))
    c.put("daily", "empty", pd.DataFrame())
    c.put("daily", "none", None)
    assert c.peek("daily", "empty") is None
    assert c.keys("daily") == []


def test_put_overwrite_same_key_keeps_single_file(tmp_path):
    c = DataCache(root=str(tmp_path))
    c.put("minute", "real_A", _minute_df())
    df2 = _minute_df()
    df2["close"] = [9.0, 9.0, 9.0]
    c.put("minute", "real_A", df2)
    out = c.peek("minute", "real_A")
    assert out["close"].iloc[0] == 9.0
    files = [f for f in os.listdir(tmp_path / "minute") if f.endswith(".parquet")]
    assert files == ["real_A.parquet"]  # 无 .tmp 残留、无重复文件


def test_get_miss_calls_loader_and_persists(tmp_path):
    c = DataCache(root=str(tmp_path))
    df = _minute_df()
    calls = []

    def loader():
        calls.append(1)
        return df

    out = c.get("minute", "real_A", loader)
    assert calls == [1]
    pd.testing.assert_frame_equal(out, df, check_freq=False)

    def _boom():
        raise AssertionError("命中缓存不应再调 loader")

    out2 = c.get("minute", "real_A", _boom)
    pd.testing.assert_frame_equal(out2, df, check_freq=False)


def test_get_all_keys_clear(tmp_path):
    c = DataCache(root=str(tmp_path))
    c.put("daily", "tushare_A", _minute_df())
    c.put("daily", "mootdx_B", _minute_df())
    assert sorted(c.keys("daily")) == ["mootdx_B", "tushare_A"]
    all_d = c.get_all("daily")
    assert set(all_d) == {"tushare_A", "mootdx_B"}
    pd.testing.assert_frame_equal(all_d["tushare_A"], _minute_df(), check_freq=False)
    c.clear("daily")
    assert c.keys("daily") == []
    assert c.get_all("daily") == {}


def test_get_all_skips_corrupted_file(tmp_path):
    c = DataCache(root=str(tmp_path))
    c.put("daily", "good", _minute_df())
    (tmp_path / "daily" / "bad.parquet").write_bytes(b"not a parquet")
    out = c.get_all("daily")
    assert set(out) == {"good"}
    # peek 对损坏文件同样按未命中处理
    assert c.peek("daily", "bad") is None


def test_peek_missing_freq_returns_empty(tmp_path):
    c = DataCache(root=str(tmp_path))
    assert c.peek("5min", "nope") is None
    assert c.keys("5min") == []
    assert c.get_all("5min") == {}
