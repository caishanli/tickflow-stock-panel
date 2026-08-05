# -*- coding: utf-8 -*-
"""DataManager 数据层已确认 bug 的修复回归测试（合成数据，离线模式）。

覆盖修复点：
- H7  get_daily_money_cached 先按 end_date 过滤、再 per-code tail(count)
      （原实现顺序颠倒，缓存延伸到"今天"时回测期内任何过去 end_date 返回空）
- H6a get_minute_feed 改走 _load_minute_merged（real_ 真实 1m 基底生效）
- H6c _minute_mem 命中校验覆盖区间（同参数结果与访问顺序无关）
- 重复定义 preload_minute_for_pool 合并为一个
- M11 _minute_mem 有界 LRU（容量上限 + 驱逐释放 + 覆盖元数据联动清理）

分钟读取已改走 network client（不再直读本地 real_ 分区），测试用
``_FakeClient`` 替身把合成 real_ 1m 帧从 client 喂入，断言仍覆盖
DataManager 的窗口裁剪/覆盖校验/滑窗/预热逻辑。
"""

import inspect

import pandas as pd
import pandas.testing as pdt

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.manager import DataManager

CODE = "510300.XSHG"
WIN_START = "2026-03-02"
WIN_END = "2026-03-31"
# real_ 真实 1m 段（含窗口外延伸，验证裁剪）
REAL_START, REAL_END = "2026-03-16 09:30", "2026-04-10 15:00"
REAL_CLOSE = 777.0    # real_ 真实段标记价


def _make_dm(tmp_path, set_window=True, **kw):
    """构造离线 DataManager：缓存落在 tmp_path，不联网。"""
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)), **kw)
    dm._offline = True
    if set_window:
        dm.set_minute_window(WIN_START, WIN_END)
    return dm


class _FakeClient:
    """网络客户端替身：get_price/get_minute_pool 从内存帧返回合成 real_ 1m 数据。

    DataManager 分钟读取已改走 network client（不再直读本地 real_ 分区），
    用替身把合成数据从 client 喂入，离线不联网。与 stockdata 服务语义对齐：
    end_date 为纯日期时按全天闭合（含当日 15:00 bar），裁剪由 DataManager
    侧完成（H6b）。
    """

    def __init__(self, frames):
        self.frames = frames  # {code: DatetimeIndex 分钟帧}

    def get_price(self, security, start_date=None, end_date=None,
                  frequency="daily", fields=None):
        codes = [security] if isinstance(security, str) else list(security)
        lo = pd.Timestamp(start_date) if start_date else None
        hi = pd.Timestamp(end_date) if end_date else None
        if hi is not None and hi == hi.normalize():
            hi = hi + pd.Timedelta(hours=15)
        out = {}
        for c in codes:
            df = self.frames.get(c)
            if df is None:
                continue
            if lo is not None:
                df = df[df.index >= lo]
            if hi is not None:
                df = df[df.index <= hi]
            out[c] = df
        return out

    def get_minute_pool(self, codes, lo_ts, hi_ts):
        return self.get_price(
            codes,
            start_date=str(lo_ts) if lo_ts is not None else None,
            end_date=str(hi_ts) if hi_ts is not None else None,
            frequency="1m")


def _real_minute_frame(code=CODE):
    idx1 = pd.date_range(REAL_START, REAL_END, freq="1min")
    return pd.DataFrame(
        {"open": REAL_CLOSE, "high": REAL_CLOSE, "low": REAL_CLOSE,
         "close": REAL_CLOSE, "volume": 1.0, "money": 1.0},
        index=idx1)


def _seed_minute_caches(dm, code=CODE):
    """注入网络客户端替身：client 返回合成 real_ 1m 帧（替代旧本地分区直读）。"""
    dm.client = _FakeClient({code: _real_minute_frame(code)})


def _daily_df(dates, money_base=1e6):
    n = len(dates)
    return pd.DataFrame({
        "trade_date": [d.strftime("%Y-%m-%d") for d in dates],
        "open": [1.0] * n,
        "close": [1.0] * n,
        "volume": [100.0] * n,
        "money": [float(money_base + i) for i in range(n)],
    })


# ---------------- H7：money 先过滤 end_date、再 per-code tail(count) ----------------

def test_money_filter_before_tail(tmp_path):
    """日线缓存延伸到"今天"时，过去的 end_date 也必须返回最近 count 行。"""
    dm = _make_dm(tmp_path, set_window=False)
    today = pd.Timestamp.today().normalize()
    dates = pd.bdate_range("2026-01-02", today)  # 缓存延伸到今天（> end_date）
    codes = ["510300.XSHG", "511880.XSHG"]
    bases = {"510300.XSHG": 1e6, "511880.XSHG": 2e6}
    for c in codes:
        dm._daily_mem[f"get_daily_{c}"] = _daily_df(dates, bases[c])

    end = min(pd.Timestamp("2026-06-30"), today)
    res = dm.get_daily_money_cached(codes, end, count=3)
    expect_dates = [d for d in dates if d <= end][-3:]
    assert len(expect_dates) == 3
    for c in codes:
        sub = res[res["code"] == c]
        # 每只恰好 end_date 前最近 3 个交易日（旧实现 tail(3) 后再过滤 → 0 行）
        assert list(sub["time"]) == list(expect_dates)
        assert list(sub["money"]) == [bases[c] + dates.get_loc(d)
                                      for d in expect_dates]


def test_money_memo_full_frame_and_version(tmp_path):
    """memo 缓存未截断全量帧：同一 codes 不同 end_date/count 各自正确；
    日线数据版本号递增（fetch/preload_daily 写路径）后 memo 重建。"""
    dm = _make_dm(tmp_path, set_window=False)
    today = pd.Timestamp.today().normalize()
    dates = pd.bdate_range("2026-01-02", today)
    dm._daily_mem[f"get_daily_{CODE}"] = _daily_df(dates, 1e6)

    res1 = dm.get_daily_money_cached([CODE], "2026-03-31", count=2)
    assert len(res1) == 2
    assert res1["time"].max() <= pd.Timestamp("2026-03-31")
    # 第二次调用换 end_date/count：memo 全量帧仍给出正确的早期窗口
    res2 = dm.get_daily_money_cached([CODE], "2026-02-27", count=3)
    assert len(res2) == 3
    assert res2["time"].max() <= pd.Timestamp("2026-02-27")
    assert res2["time"].max() < res1["time"].max()

    # 模拟 fetch 写路径：_daily_mem 变更 + 版本号递增 → memo 必须重建
    dm._daily_mem[f"get_daily_{CODE}"] = _daily_df(dates, 9e8)
    dm._daily_ver += 1
    res3 = dm.get_daily_money_cached([CODE], "2026-03-31", count=2)
    assert (res3["money"] >= 9e8).all()


# ---------------- H6b：merged 结果裁剪到请求窗口 [lo, hi] ----------------

def test_merged_clipped_to_window_and_real_only(tmp_path):
    """merged 只含真实 mootdx 数据；越出窗口裁剪。"""
    dm = _make_dm(tmp_path)
    _seed_minute_caches(dm)
    merged = dm._load_minute_merged(CODE, full=True)
    assert not merged.empty
    # 上界不越出回测末端（real_ 缓存延伸到 04-10，应被裁掉）
    assert merged.index.max() == pd.Timestamp("2026-03-31 15:00")
    assert merged.index.min() >= pd.Timestamp("2026-03-16 09:30")
    # real_ 真实段保留真实值
    assert merged.loc[pd.Timestamp("2026-03-17 10:00"), "close"] == REAL_CLOSE
    # 不再包含 baostock 插值数据（real 之前的缺口不补齐）
    assert pd.Timestamp("2026-03-03 10:00") not in merged.index


# ---------------- H6a：get_minute_feed 走 merged（real_ 基底） ----------------

def test_minute_feed_uses_real_base(tmp_path):
    """feed 路径只含真实 mootdx 数据，不再有 baostock 插值。"""
    dm = _make_dm(tmp_path)
    _seed_minute_caches(dm)
    feed = dm.get_minute_feed(CODE, WIN_START, WIN_END)
    assert not feed.empty
    # 只含真实 mootdx 数据
    assert feed.loc[pd.Timestamp("2026-03-17 10:00"), "close"] == REAL_CLOSE
    # real 之前的缺口不再由 baostock 补齐
    assert pd.Timestamp("2026-03-03 10:00") not in feed.index
    assert feed.index.max() <= pd.Timestamp("2026-03-31 15:00")


# ---------------- H6c：命中校验覆盖区间，结果与访问顺序无关 ----------------

def test_feed_reloads_when_cached_window_insufficient(tmp_path):
    """滑窗帧先入缓存后再请求整段 feed：覆盖不足必须重新合并（不先到先得）。"""
    dm = _make_dm(tmp_path)
    _seed_minute_caches(dm)
    # 先走策略滑窗：覆盖 [03-10, 03-25 15:00]
    dm._ensure_minute_windowed(CODE, "2026-03-25")
    assert dm._minute_cov[CODE][1] == pd.Timestamp("2026-03-25 15:00")
    # 再请求整段 feed [03-02, 03-31]：只含真实 mootdx 数据
    feed = dm.get_minute_feed(CODE, WIN_START, WIN_END)
    # real 之前的缺口不再由 baostock 补齐
    assert pd.Timestamp("2026-03-03 10:00") not in feed.index
    assert feed.loc[pd.Timestamp("2026-03-17 10:00"), "close"] == REAL_CLOSE


def test_feed_result_independent_of_access_order(tmp_path):
    """同参数回测结果可复现：先滑窗后 feed 与直接 feed 结果完全一致。"""
    dm1 = _make_dm(tmp_path)
    _seed_minute_caches(dm1)
    dm1._ensure_minute_windowed(CODE, "2026-03-25")  # 先污染缓存
    feed1 = dm1.get_minute_feed(CODE, WIN_START, WIN_END)

    dm2 = _make_dm(tmp_path)
    _seed_minute_caches(dm2)
    feed2 = dm2.get_minute_feed(CODE, WIN_START, WIN_END)  # 直接 feed
    pdt.assert_frame_equal(feed1, feed2)


def test_sliding_window_reloads_when_as_of_advances(tmp_path):
    """滑窗随 as_of 前移：覆盖不足时重新加载，新窗口含更晚数据。"""
    dm = _make_dm(tmp_path)
    _seed_minute_caches(dm)
    df1 = dm._ensure_minute_windowed(CODE, "2026-03-20")
    assert df1.index.max() == pd.Timestamp("2026-03-20 15:00")
    df2 = dm._ensure_minute_windowed(CODE, "2026-03-30")
    assert dm._minute_cov[CODE][1] == pd.Timestamp("2026-03-30 15:00")
    assert df2.index.max() == pd.Timestamp("2026-03-30 15:00")
    # 同一 as_of 再次调用直接命中（不逐 bar 重载）
    assert dm._ensure_minute_windowed(CODE, "2026-03-30") is df2


# ---------------- 重复定义合并：preload_minute_for_pool 只有一个 ----------------

def test_preload_minute_for_pool_single_definition():
    src = inspect.getsource(DataManager)
    assert src.count("def preload_minute_for_pool") == 1
    sig = inspect.signature(DataManager.preload_minute_for_pool)
    assert sig.parameters["as_of"].default is None


def test_preload_minute_for_pool_functional(tmp_path):
    """合并后的实现：as_of 可缺省语义保留；预热帧按滑窗裁剪并记录覆盖。"""
    dm = _make_dm(tmp_path)
    _seed_minute_caches(dm)
    dm.preload_minute_for_pool([CODE], as_of="2026-03-25")
    df = dm._minute_mem.get(CODE)
    assert df is not None and not df.empty
    assert df.index.max() == pd.Timestamp("2026-03-25 15:00")
    assert dm._minute_cov[CODE][1] == pd.Timestamp("2026-03-25 15:00")


# ---------------- M11：_minute_mem 有界 LRU ----------------

def test_minute_mem_lru_cap_and_eviction(tmp_path):
    """超出容量逐出最久未用项，并连带清理 _minute_cov；get 命中刷新热度。"""
    dm = _make_dm(tmp_path, minute_mem_cap=2)
    idx = pd.date_range("2026-03-16 09:30", "2026-03-16 10:30", freq="1min")
    f = pd.DataFrame({"close": 1.0}, index=idx)
    t = pd.Timestamp("2026-03-16")
    for c in ("a", "b"):
        dm._minute_mem[c] = f
        dm._minute_cov[c] = (t, t)
    dm._minute_mem.get("a")  # touch：b 变为最久未用
    dm._minute_mem["c"] = f  # 触发驱逐
    dm._minute_cov["c"] = (t, t)
    assert set(dm._minute_mem.keys()) == {"a", "c"}
    assert len(dm._minute_mem) <= 2
    assert "b" not in dm._minute_cov  # 驱逐时覆盖元数据联动清理


def test_minute_mem_default_cap(tmp_path):
    """容量默认 800（与 scripts/run_jq_rqalpha.py --minute_cache_cap 一致）。"""
    dm = _make_dm(tmp_path)
    assert dm._minute_mem.cap == 800
