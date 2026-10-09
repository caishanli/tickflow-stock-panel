"""竞价采集管线回归：fetch_and_save 必须在 pandas instruments 上工作。

回归（2026-10-09 发现）：stockdata 竞价 cron 每个工作日 09:25:09 都触发，
但每天都失败——`instruments_frame()` 返回 pandas DataFrame，
而 `fetch_and_save` 调 Polars 的 `.iter_rows()` → AttributeError，
auction_cache 永远只有一天数据，get_call_auction 的 09:25 精确通路全废。
"""
import datetime

import pandas as pd

from app.quant import auction_cache


class _FakeInstruments:
    """pandas instruments 帧（生产中 instruments_frame 的真实返回类型）。"""


def _install(monkeypatch, tmp_path):
    import app.quant.stock_meta as sm

    monkeypatch.setattr(sm, "instruments_frame",
                        lambda: pd.DataFrame({"symbol": ["000001.XSHE", "600000.XSHG"]}))
    monkeypatch.setattr(
        auction_cache, "fetch_tencent_auction",
        lambda codes: {c: {"price": 10.5, "volume_shares": 123400.0,
                           "prev_close": 10.0, "open": 10.5} for c in codes})
    monkeypatch.setattr(auction_cache, "CACHE_ROOT", str(tmp_path / "auction_cache"))
    auction_cache._memory_cache.clear()


def test_fetch_and_save_pandas_instruments(monkeypatch, tmp_path):
    """instruments_frame 返回 pandas 帧时，fetch_and_save 应正常落盘。"""
    _install(monkeypatch, tmp_path)
    day = datetime.date(2026, 10, 12)
    df = auction_cache.fetch_and_save(day=day)
    assert not df.empty
    assert set(df["symbol"]) == {"000001.XSHE", "600000.XSHG"}
    # 落盘 + lookup 可读回（get_call_auction 的精确通路）
    got = auction_cache.lookup("600000.XSHG", day)
    assert got == (123400.0, 10.5)


def test_lookup_missing_day_returns_none(monkeypatch, tmp_path):
    _install(monkeypatch, tmp_path)
    assert auction_cache.lookup("600000.XSHG", datetime.date(2026, 10, 13)) is None
