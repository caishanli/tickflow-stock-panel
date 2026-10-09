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


def test_auction_0930_bar_needs_no_prev_volume():
    """09:30 精确 bar 不得因 prev_day_vol 缺失而返回 NaN 量。

    回归（2026-10-09 实盘路径实测）：prev_day_vol 守卫原在 0930 分支之前，
    日线帧未缓存（get_call_auction 首调）时 09:30 真实竞价量白丢。
    """
    import numpy as np

    from app.quant.stock_meta import auction_from_day_bars

    hm = np.array([930, 931, 932, 933], dtype=int)
    op = np.array([9.5, 9.6, 9.7, 9.8])
    cl = np.array([9.5, 9.6, 9.7, 9.8])
    vol = np.array([9_235_400.0, 31_531_100.0, 17_404_700.0, 9_077_900.0])
    auc, cur = auction_from_day_bars(hm, op, cl, vol, float("nan"))
    assert auc == 9_235_400.0
    assert cur == 9.5


# ---------------------------------------------------------------- ensure_auction
_IN_WINDOW = datetime.datetime(2026, 10, 12, 9, 26)      # 周一 09:26 竞价窗口内
_OUT_WINDOW = datetime.datetime(2026, 10, 12, 10, 0)     # 开盘后
_PRE_WINDOW = datetime.datetime(2026, 10, 12, 9, 24)     # 撮合完成前


def _seed_day(day, rows):
    import os

    d = os.path.join(auction_cache.CACHE_ROOT, f"date={day.isoformat()}")
    os.makedirs(d, exist_ok=True)
    pd.DataFrame(rows).to_parquet(os.path.join(d, "part.parquet"), index=False)
    auction_cache._memory_cache.clear()


def test_ensure_auction_merges_without_dropping_cached(monkeypatch, tmp_path):
    """按需抓取必须增量合并：不能只写新帧把当日已有行冲掉。"""
    monkeypatch.setattr(auction_cache, "CACHE_ROOT", str(tmp_path / "ac"))
    auction_cache._memory_cache.clear()
    day = datetime.date(2026, 10, 12)
    _seed_day(day, [{"symbol": "600000.XSHG", "auction_price": 10.0,
                     "auction_volume": 100000.0, "prev_close": 9.9}])
    monkeypatch.setattr(auction_cache, "fetch_tencent_auction", lambda codes: {
        "600000.XSHG": {"price": 10.2, "volume_shares": 120000.0,
                        "prev_close": 9.9, "open": 10.2},
        "000001.XSHE": {"price": 11.0, "volume_shares": 200000.0,
                        "prev_close": 10.8, "open": 11.0},
    })
    n = auction_cache.ensure_auction(["000001.XSHE"], day, now=_IN_WINDOW)
    assert n == 2  # 缺失的 000001 抓到；600000 一并刷新（窗口内只会更完整）
    assert auction_cache.lookup("600000.XSHG", day) == (120000.0, 10.2)
    assert auction_cache.lookup("000001.XSHE", day) == (200000.0, 11.0)


def test_ensure_auction_skips_when_all_cached(monkeypatch, tmp_path):
    """本地已有 → 不触网（这才是"本地没有才去网络拿"的语义）。"""
    monkeypatch.setattr(auction_cache, "CACHE_ROOT", str(tmp_path / "ac"))
    auction_cache._memory_cache.clear()
    day = datetime.date(2026, 10, 12)
    _seed_day(day, [{"symbol": "600000.XSHG", "auction_price": 10.0,
                     "auction_volume": 100000.0, "prev_close": 9.9}])

    def _boom(codes):
        raise AssertionError("cached codes must not hit network")

    monkeypatch.setattr(auction_cache, "fetch_tencent_auction", _boom)
    assert auction_cache.ensure_auction(["600000.XSHG"], day, now=_IN_WINDOW) == 0


def test_ensure_auction_outside_window_never_fetches(monkeypatch, tmp_path):
    """窗口外腾讯现价≠竞价撮合价（累计量含连续竞价）→ 禁止抓取。"""
    monkeypatch.setattr(auction_cache, "CACHE_ROOT", str(tmp_path / "ac"))
    auction_cache._memory_cache.clear()
    day = datetime.date(2026, 10, 12)

    def _boom(codes):
        raise AssertionError("outside auction window must not hit network")

    monkeypatch.setattr(auction_cache, "fetch_tencent_auction", _boom)
    assert auction_cache.ensure_auction(["600000.XSHG"], day, now=_OUT_WINDOW) == 0
    assert auction_cache.ensure_auction(["600000.XSHG"], day, now=_PRE_WINDOW) == 0
    assert auction_cache.ensure_auction(["600000.XSHG"], day,
                                        now=datetime.datetime(2026, 10, 10, 9, 26)) == 0
