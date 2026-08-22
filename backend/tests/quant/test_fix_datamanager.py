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


class _StrictMinuteClient(_FakeClient):
    """忠实模拟 stockdata 服务端 ``h_get_minute``：纯日期上界**不**自动补 15:00，
    精确按 ``datetime <= hi_ts`` 过滤（与 ``h_get_price`` 分钟路径补 15:00 不同）。

    用于复现 DataManager ``preload_minute_for_pool`` 把窗口上界（纯日期午夜）
    原样传给网络端时，当天分钟被整段排除、而 ``_minute_cov`` 却记为覆盖到
    当日 15:00 的覆盖区间假阳性。
    """

    def get_minute_pool(self, codes, lo_ts, hi_ts):
        lo = pd.Timestamp(lo_ts) if lo_ts is not None else None
        hi = pd.Timestamp(hi_ts) if hi_ts is not None else None
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


def test_money_window_aligns_to_trade_days_not_code_rows(tmp_path):
    """get_daily_money_cached 窗口须按「end_date 前最近 count 个交易日」对齐，
    而非 per-code 数据行的最后 count 行。

    回归：已退市/停牌 ETF（如 560650，7/2 起 amount 为 sentinel 被剔除）的
    ``tail(count)`` 会取到停牌前的陈旧日期（06-29/06-30/07-01），拖进策略
    "全市场ETF总成交额"日志 → "0.00亿元 (1只ETF有成交)" 误导，且陈旧日期
    混入均值拉低流动性阈值。窗口应只含 end_date 前的真实交易日。"""
    dm = _make_dm(tmp_path, set_window=False)
    # 交易日：仅 07-01 ~ 07-31（bdate_range 含周末跳过的交易日）
    dates = pd.bdate_range("2026-07-01", "2026-07-31")
    # 正常 ETF：全窗口有数据
    dm._daily_mem["get_daily_510300.XSHG"] = _daily_df(dates, 1e6)
    # 退市 ETF：仅 07-01~07-03 有正常成交，之后停牌（数据缺失/被过滤）
    dm._daily_mem["get_daily_560650.XSHG"] = _daily_df(dates[:3], 1e3)

    end = "2026-07-20"
    res = dm.get_daily_money_cached(["510300.XSHG", "560650.XSHG"], end, count=3)
    # 窗口应是 end_date 前最近 3 个交易日（07-16/17/20），而非各 code 的最后 3 行
    expect_dates = [d for d in dates if d.date() <= pd.Timestamp(end).date()][-3:]
    got_dates = sorted({d.date() for d in res["time"]})
    assert [d.strftime("%Y-%m-%d") for d in got_dates] == \
        [d.strftime("%Y-%m-%d") for d in expect_dates], f"窗口错误: {got_dates}"
    # 退市 ETF 在窗口内无数据 → 缺席，不产生陈旧日期行
    assert "560650.XSHG" not in set(res["code"])
    # 正常 ETF 在窗口内完整返回 count 行
    sub = res[res["code"] == "510300.XSHG"]
    assert len(sub) == 3
    assert list(sub["time"]) == list(pd.DatetimeIndex(expect_dates))


def test_money_filter_excludes_placeholder_amount(tmp_path):
    """停牌/退市标的分区里 amount=2**-127（≈0 sentinel）不得算"有成交"。

    回归：560650.SH 停牌后 07-02~07-30 amount 恒为 5.877e-39（占位值），
    _build_money_full 的 ``money > 0`` 会把它计入 → 策略"全市场ETF总成交额"
    出现 "0.00亿元 (1只ETF有成交)" 的误导日志。占位 amount 应整体剔除。"""
    dm = _make_dm(tmp_path, set_window=False)
    today = pd.Timestamp.today().normalize()
    dates = pd.bdate_range("2026-07-01", min(pd.Timestamp("2026-08-06"), today))
    # 正常 ETF：真实成交额
    dm._daily_mem["get_daily_510300.XSHG"] = _daily_df(dates, 1e6)
    # 占位 ETF：07-02 起 amount 为 sentinel（≈0）
    n = len(dates)
    stub = pd.DataFrame({
        "trade_date": [d.strftime("%Y-%m-%d") for d in dates],
        "open": [1.068] * n,
        "close": [1.068] * n,
        "volume": [2 ** -125] * n,
        "money": [2 ** -127] * n,
    })
    dm._daily_mem["get_daily_560650.XSHG"] = stub

    res = dm.get_daily_money_cached(["510300.XSHG", "560650.XSHG"],
                                    str(dates[-1].date()), count=3)
    assert "560650.XSHG" not in set(res["code"]), \
        "占位 amount 标的不应出现在成交额明细中"
    # 正常 ETF 仍完整返回
    assert "510300.XSHG" in set(res["code"])


# ---------------- preload_daily：新交易日（asof 前移）必须重新预载 ----------------

def _daily_through(ends, money=1e6):
    """构建截至 ``ends``（date）的合成日线帧（trade_date 列 + amount）。"""
    idx = pd.bdate_range("2026-07-01", ends)
    n = len(idx)
    return pd.DataFrame({
        "trade_date": [d.strftime("%Y-%m-%d") for d in idx],
        "open": [1.0] * n,
        "high": [1.0] * n,
        "low": [1.0] * n,
        "close": [1.0] * n,
        "volume": [100.0] * n,
        "amount": [float(money)] * n,
    })


class _PreloadAdvanceClient:
    """模拟 stockdata 服务：preload_daily 按 asof 返回截至该日的全量日线帧。"""

    def __init__(self, codes):
        self.codes = codes
        self.preload_asofs = []

    def preload_daily(self, lookback_days=400, asof=None):
        self.preload_asofs.append(asof)
        return {c: _daily_through(asof) for c in self.codes}

    def get_price(self, security, start_date=None, end_date=None,
                  frequency="daily", fields=None):
        hi = pd.Timestamp(end_date).date() if end_date else pd.Timestamp.today().date()
        codes = [security] if isinstance(security, str) else list(security)
        return {c: _daily_through(hi) for c in codes if c in self.codes}


def test_preload_daily_reloads_when_asof_advances(tmp_path, monkeypatch):
    """preload_daily 幂等标志不得按进程生命周期锁死：新交易日到来（asof 前移）
    盘前必须重新预载，让日线帧延伸到最新已完成交易日。

    回归：模拟盘 08-13 22:39 重启后补跑（preload asof=08-12），进入实时次日
    08-14 盘前 _pre_market 的 preload_daily() 因 ``_daily_preloaded=True`` 直接
    跳过，``_daily_mem`` 停留在 08-12；09:31 策略算"全市场ETF总成交额"时最新
    交易日 08-13 只有被零星刷新的子集有数据 → 日志出现 "2026-08-13 ... 
    (225只ETF有成交)"，3 日均值/流动性阈值被拉低。修复：按 asof 判幂等，新
    交易日自动重载。
    """
    from app.quant.jqengine.datasource import manager as mgr_mod

    codes = ["510300.XSHG", "511880.XSHG", "159915.XSHE"]
    client = _PreloadAdvanceClient(codes)
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)), client=client)
    dm._offline = False

    # 08-13 22:39 重启：盘前 preload asof=08-12
    monkeypatch.setattr(mgr_mod.pd.Timestamp, "now",
                        classmethod(lambda cls, tz=None: pd.Timestamp("2026-08-13 22:40:00")))
    dm.preload_daily()
    assert dm._daily_preloaded_asof == pd.Timestamp("2026-08-12").date()
    assert client.preload_asofs == [pd.Timestamp("2026-08-12").date()]
    for c in codes:
        assert str(dm._daily_mem[f"get_daily_{c}"]["trade_date"].max()) == "2026-08-12"

    # 次日 08-14 盘前：asof 前移到 08-13，必须重新预载
    monkeypatch.setattr(mgr_mod.pd.Timestamp, "now",
                        classmethod(lambda cls, tz=None: pd.Timestamp("2026-08-14 09:25:00")))
    dm.preload_daily()
    assert dm._daily_preloaded_asof == pd.Timestamp("2026-08-13").date()
    assert client.preload_asofs[-1] == pd.Timestamp("2026-08-13").date()
    for c in codes:
        assert str(dm._daily_mem[f"get_daily_{c}"]["trade_date"].max()) == "2026-08-13"

    # 同日再次调用幂等（asof 不变，不重复预载）
    n = len(client.preload_asofs)
    dm.preload_daily()
    assert len(client.preload_asofs) == n

    # 修复后完整性：09:31 算最新交易日成交额，全量标的都含 08-13
    out = dm.get_daily_money_cached(codes, end_date="2026-08-13", count=3)
    days = sorted({str(t.date()) for t in out["time"]})
    assert days[-1] == "2026-08-13"
    for c in codes:
        sub = out[out["code"] == c]
        assert {str(t.date()) for t in sub["time"]} == {"2026-08-11", "2026-08-12", "2026-08-13"}


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


def test_preload_pool_upper_bound_includes_close_bar(tmp_path):
    """补跑钉窗时 preload_minute_for_pool 的批量取数必须把窗口上界扩到当日
    15:00（而非纯日期午夜），否则服务端 get_minute 会把当天分钟整段排除、
    _minute_cov 却记为覆盖到 15:00 → 收盘重估取到昨日价。

    回归：模拟盘 517520.XSHG 8-07 收盘后补跑，_revalue_at_close 经
    get_minute_price_at 取到 8-06 收盘 2.031（真实 8-07 收盘 2.112），
    净值低估 ~3904。
    """
    dm = _make_dm(tmp_path)
    # 钉窗结束于"今天"（纯日期），复现补跑 today 场景
    dm.set_minute_window("2026-08-03", "2026-08-07")
    # 真实分钟帧：8-06 收 2.031，8-07 收 2.112（最后 15:00 bar）
    idx1 = pd.date_range("2026-08-03 09:31", "2026-08-06 15:00", freq="1min")
    idx2 = pd.date_range("2026-08-07 09:31", "2026-08-07 15:00", freq="1min")
    df = pd.concat([
        pd.DataFrame({"close": 2.031, "volume": 1.0, "money": 1.0}, index=idx1),
        pd.DataFrame({"close": 2.112, "volume": 1.0, "money": 1.0}, index=idx2),
    ])
    dm.client = _StrictMinuteClient({CODE: df})
    dm.preload_minute_for_pool([CODE], as_of="2026-08-07")
    cached = dm._minute_mem.get(CODE)
    assert cached is not None and not cached.empty
    # 覆盖区间与帧必须一致：缓存帧含 8-07 收盘 bar
    assert cached.index.max() == pd.Timestamp("2026-08-07 15:00")
    # 收盘重估/取价必须命中 8-07 收盘价，而非昨日 2.031
    assert dm.get_minute_price_at(CODE, "2026-08-07 15:00") == 2.112


def test_unset_minute_window_restores_sliding(tmp_path):
    """补跑结束后必须复位分钟窗口：钉窗泄漏到实时模式会让
    preload_minute_for_pool 永远用补跑区间的 full 窗口（as_of 前移也不滑动），
    逐日重新加载丢失批量预取。unset 后回到滑窗语义。"""
    dm = _make_dm(tmp_path)
    _seed_minute_caches(dm)
    dm.set_minute_window("2026-03-02", "2026-03-31")
    assert dm._minute_win is not None
    dm.unset_minute_window()
    assert dm._minute_win is None
    # unset 后 preload 走滑窗（full=False）：as_of 前移，上界跟随 as_of
    _seed_minute_caches(dm)
    dm.preload_minute_for_pool([CODE], as_of="2026-03-25")
    df = dm._minute_mem.get(CODE)
    assert df is not None and not df.empty
    assert df.index.max() == pd.Timestamp("2026-03-25 15:00")
    assert dm._minute_cov[CODE][1] == pd.Timestamp("2026-03-25 15:00")


def test_preload_minute_for_pool_functional(tmp_path):
    """合并后的实现：as_of 可缺省语义保留；回测（已 set_minute_window）加载整段
    回测窗口并记录覆盖（T17：整窗帧供 get_minute_feed 命中，免逐标的全窗口联网）；
    未设窗口（实时/模拟盘）保持滑窗裁剪。"""
    dm = _make_dm(tmp_path)
    _seed_minute_caches(dm)
    dm.preload_minute_for_pool([CODE], as_of="2026-03-25")
    df = dm._minute_mem.get(CODE)
    assert df is not None and not df.empty
    # 已 set_minute_window → 整段回测窗口（superset of 滑窗）
    assert df.index.max() == pd.Timestamp(WIN_END + " 15:00")
    assert dm._minute_cov[CODE][1] == pd.Timestamp(WIN_END + " 15:00")
    # 无窗口（实时/模拟盘）→ 滑窗截至 as_of
    dm2 = _make_dm(tmp_path, set_window=False)
    _seed_minute_caches(dm2)
    dm2.preload_minute_for_pool([CODE], as_of="2026-03-25")
    df2 = dm2._minute_mem.get(CODE)
    assert df2 is not None and not df2.empty
    assert df2.index.max() == pd.Timestamp("2026-03-25 15:00")
    assert dm2._minute_cov[CODE][1] == pd.Timestamp("2026-03-25 15:00")


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


# ---------------- 分钟数据覆盖告警：回测起点早于分钟数据首日 ----------------

def test_minute_coverage_start(tmp_path, monkeypatch):
    """最早分钟分区日期：无分区返回 None；以 ETF 分钟覆盖为准（回测宇宙为
    ETF），股票分钟（kline_minute，2018 起）仅作无 ETF 目录时的兜底——
    避免股票分钟的历史覆盖掩盖 ETF 分钟缺口导致告警漏报。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    dm = _make_dm(tmp_path)
    assert dm.minute_coverage_start() is None
    (tmp_path / "kline_etf_minute" / "date=2026-04-01").mkdir(parents=True)
    assert dm.minute_coverage_start() == pd.Timestamp("2026-04-01").date()
    (tmp_path / "kline_minute" / "date=2018-02-09").mkdir(parents=True)
    assert dm.minute_coverage_start() == pd.Timestamp("2026-04-01").date()
    # 无 ETF 分钟目录时兜底到股票分钟
    import shutil
    shutil.rmtree(tmp_path / "kline_etf_minute")
    assert dm.minute_coverage_start() == pd.Timestamp("2018-02-09").date()


# ---------------- fetch 缓存只存日线 DataFrame；元数据（list）不缓存 ----------------

def test_fetch_etf_list_is_not_cached_as_daily(tmp_path):
    """回归：mgr.fetch('get_etf_list') 返回 list，绝不能写入 _daily_mem。

    旧实现把它塞进 _daily_mem[get_etf_list_]，二次 fetch 命中缓存分支后
    DataCache._covers 对 list 调 df.columns → AttributeError → get_all_securities
    兜底返回空 → 「未找到任何场内ETF / 无ETF通过流动性过滤」。修复后：
    只对 DataFrame 写缓存；非 DataFrame 每次透传，可重复调用且不残留缓存。
    """
    class _ListClient:
        def get_etf_list(self):
            return ["510300.XSHG", "159915.XSHE"]

        def get_stock_list(self):
            return ["600000.XSHG"]
        # _build_money_full / fetch 需要 get_daily 路径，提供空实现避免误用
        def get_daily(self, code, start, end):
            return pd.DataFrame()

    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)))
    dm.sources = {"network": _ListClient()}
    dm._offline = False

    r1 = dm.fetch("get_etf_list")
    r2 = dm.fetch("get_etf_list")
    r3 = dm.fetch("get_etf_list")
    assert r1 == r2 == r3 == ["510300.XSHG", "159915.XSHE"]
    # 核心断言：非 DataFrame 值不落 _daily_mem
    assert "get_etf_list_" not in dm._daily_mem
    assert dm._daily_ver == 0  # 非 DataFrame 写入不 bump 版本号


def test_fetch_daily_caches_dataframe_only(tmp_path):
    """get_daily 返回 DataFrame 才写 _daily_mem；已写脏 list 缓存能在命中分支自愈。"""
    calls = {"n": 0}

    class _C:
        def get_daily(self, code, start, end):
            calls["n"] += 1
            idx = pd.date_range("2026-01-01", periods=3, freq="D")
            return pd.DataFrame(
                {"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                 "volume": 1000.0, "money": 1e7},
                index=idx)

    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)))
    dm.sources = {"network": _C()}
    dm._offline = False
    dm._daily_mem["get_daily_510300.XSHG"] = ["dirty.list"]  # 模拟旧版脏缓存
    # 命中分支必须是 DataFrame 才信任；脏 list 应视为未命中 → 删除并回源
    df = dm.fetch("get_daily", "510300.XSHG", "2026-01-01", "2026-01-03")
    assert calls["n"] == 1  # 脏缓存未命中，回源取数
    assert isinstance(df, pd.DataFrame) and len(df) == 3
    assert "get_daily_510300.XSHG" in dm._daily_mem  # 回源结果已按 DataFrame 重新缓存
    # 再次 fetch 命中缓存，不重复回源
    dm.fetch("get_daily", "510300.XSHG", "2026-01-01", "2026-01-03")
    assert calls["n"] == 1


# ---------------- 当日盘中分钟暂缺不得进 _minute_empty（08-13 案例） ----------------

class _DelayedMinuteClient(_FakeClient):
    """当日盘中模拟：``get_price``（只读 get_minute）返回空，但实时
    ``current_snapshot`` 能返回当日分钟。

    复现 08-13 模拟盘：进程启动回补到"今天"时，持仓标的当日分钟尚未被
    实时 feed 填充，``get_minute``（只读分区+内存库）返回空 → 旧实现把标的
    永久加入 ``_minute_empty``，且直接返回空 → 13:10 动量计算跳过它 →
    静默换仓买错。修复：盘中取数空应回退 ``current_snapshot`` 实时回源。
    """

    def __init__(self, frames, snap_frames=None):
        super().__init__(frames)
        self.snap_frames = snap_frames if snap_frames is not None else frames

    def get_price(self, security, start_date=None, end_date=None,
                  frequency="daily", fields=None):
        # 只读 get_minute 路径（_load_minute_from_partitions）：盘中当日分区未
        # 落盘、内存库未填充 → 返回空，模拟真实服务端行为
        return {}

    def current_snapshot(self, codes, as_of=None):
        out = {}
        for c in ([security] if isinstance(codes, str) else list(codes)):
            df = self.snap_frames.get(c)
            if df is None:
                continue
            if as_of is not None:
                df = df[df.index <= pd.Timestamp(as_of)]
            out[c] = df
        return out


def test_minute_today_intraday_falls_back_to_realtime(tmp_path, monkeypatch):
    """盘中当日分钟暂缺（get_minute 空）应回退 current_snapshot 实时回源取数。

    回归：08-13 新启动账户回补到当天时，159768 的 get_minute 只读路径返回空
    → 旧实现直接返回空、且把标的永久缓存进 _minute_empty → 13:10 动量计算
    判成"临时停牌"静默换仓买 513360。修复后盘中应实时回源拿到当日分钟，
    且不污染 _minute_empty。
    """
    from app.quant.jqengine.datasource import manager as mgr_mod

    today = "2026-03-25"
    monkeypatch.setattr(mgr_mod.pd.Timestamp, "now",
                        classmethod(lambda cls, tz=None: pd.Timestamp(f"{today} 12:00:00")))
    dm = _make_dm(tmp_path)
    frame = _real_minute_frame(CODE)
    # get_price 恒空（模拟盘中当日分区未落盘），但 current_snapshot 有实时数据
    dm.client = _DelayedMinuteClient({}, snap_frames={CODE: frame})
    df = dm._ensure_minute_windowed(CODE, today)
    # 盘中必须通过实时回源拿到当日分钟，而非直接返回空
    assert df is not None and not df.empty
    assert df.index.max() <= pd.Timestamp(f"{today} 15:00")
    # 且不得污染 _minute_empty（活跃标的不被永久标记）
    assert CODE not in dm._minute_empty


def test_minute_history_missing_still_marks_empty(tmp_path, monkeypatch):
    """历史日期分钟缺失仍应加入 _minute_empty（真无数据，避免重复请求）。

    与盘中暂缺不同：历史日（非今天）get_minute 空 → 停牌/退市/无数据，
    应缓存 _minute_empty 防重复联网。
    """
    from app.quant.jqengine.datasource import manager as mgr_mod

    today = "2026-03-25"
    monkeypatch.setattr(mgr_mod.pd.Timestamp, "now",
                        classmethod(lambda cls, tz=None: pd.Timestamp(f"{today} 12:00:00")))
    dm = _make_dm(tmp_path)
    dm.client = _DelayedMinuteClient({})  # 无任何数据
    df = dm._ensure_minute_windowed(CODE, "2026-03-20")  # 历史日期
    assert df is None
    assert CODE in dm._minute_empty


def test_preload_pool_intraday_falls_back_to_realtime_batch(tmp_path, monkeypatch):
    """preload_minute_for_pool 盘中批量取数空 → 批量 current_snapshot 实时回源。

    回归：08-13 新启动账户午盘 preload 时，159768 的批量 get_minute 空
    （当日分区未落盘），若仅回退单标的会导致数百次实时回源调用拖慢午盘；
    且不实时回源则 159768 被判"临时停牌"静默换仓。修复：批量实时回源。
    """
    from app.quant.jqengine.datasource import manager as mgr_mod

    today = "2026-03-25"
    monkeypatch.setattr(mgr_mod.pd.Timestamp, "now",
                        classmethod(lambda cls, tz=None: pd.Timestamp(f"{today} 12:00:00")))
    dm = _make_dm(tmp_path)
    frame = _real_minute_frame(CODE)
    # get_price（只读批量 get_minute）恒空，但 current_snapshot 能返回当日分钟
    dm.client = _DelayedMinuteClient({}, snap_frames={CODE: frame})
    dm.preload_minute_for_pool([CODE], as_of=pd.Timestamp(f"{today} 13:10:00"))
    # 批量实时回源后缓存应命中（非空）
    df = dm._minute_mem.get(CODE)
    assert df is not None and not df.empty
    assert dm._minute_cov[CODE][1] >= pd.Timestamp(f"{today} 13:10:00")
    assert CODE not in dm._minute_empty


# ---------------- 批量回源 + 默认窗口收窄（stockdata CPU 风暴修复） ----------------
#
# 背景：_build_money_full 对缓存缺失的标的逐只 fetch("get_daily", code)，
# 单参调用被补 "2000-01-01"→今天 的全区间 → 服务端 get_daily 每次顺序扫
# 全部日分区文件（~6300 个），DayFileCache(cap=60) 几乎全 miss，1600 只
# × 全历史扫描把 stockdata CPU 打满数小时。修复：
# 1) 缺失标的合并为一次 get_daily_batch 批量请求（服务端日文件只扫一遍）；
# 2) 单参兜底窗口收窄为回看 400 天（对齐 preload_daily 口径；覆盖不足时
#    下游 _covers 会带显式日期重取，不丢数据）。

BATCH_CODES = ["510300.XSHG", "511880.XSHG", "159915.XSHE"]


def _fresh_dates():
    """截至今天的合成交易日序列（避开 _is_stale 判过期）。"""
    return pd.bdate_range("2026-07-01", pd.Timestamp.today().normalize())


class _PerCodeOnlySource:
    """只有逐只 get_daily 的网络源替身（模拟不支持批量的旧源/降级路径）。"""

    def __init__(self, daily_dates):
        self.daily_dates = daily_dates
        self.daily_calls = []

    def _frame(self, code):
        idx = self.daily_dates.get(code)
        if idx is None:
            return pd.DataFrame()
        return _daily_df(idx, 2e6)

    def get_daily(self, code, start, end):
        self.daily_calls.append((code, start, end))
        return self._frame(code)


class _BatchSource(_PerCodeOnlySource):
    """支持 get_daily_batch 的网络源替身：记录批量调用，可注入失败。"""

    def __init__(self, daily_dates, fail_batch=False):
        super().__init__(daily_dates)
        self.batch_calls = []
        self.fail_batch = fail_batch

    def get_daily_batch(self, codes, start, end):
        self.batch_calls.append((list(codes), start, end))
        if self.fail_batch:
            raise RuntimeError("batch down")
        return {c: self._frame(c) for c in codes}


def _live_dm(tmp_path, source):
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)))
    dm.sources = {"network": source}
    dm._offline = False
    return dm


def test_build_money_full_batches_missing_codes(tmp_path):
    """缓存全缺：N 只缺失标的合并为一次批量请求，不逐只回源。"""
    src = _BatchSource({c: _fresh_dates() for c in BATCH_CODES})
    dm = _live_dm(tmp_path, src)
    res = dm.get_daily_money_cached(BATCH_CODES, "2026-07-20", count=3)
    assert len(src.batch_calls) == 1, f"应只发一次批量请求: {src.batch_calls}"
    assert sorted(src.batch_calls[0][0]) == sorted(BATCH_CODES)
    assert src.daily_calls == [], "批量成功时不得降级逐只"
    for c in BATCH_CODES:
        assert c in set(res["code"])


def test_build_money_full_batch_only_requests_missing(tmp_path):
    """Phase A 已命中的标的不进批量请求。"""
    dates = _fresh_dates()
    src = _BatchSource({BATCH_CODES[1]: dates})
    dm = _live_dm(tmp_path, src)
    dm._daily_mem[f"get_daily_{BATCH_CODES[0]}"] = _daily_df(dates, 1e6)
    dm.get_daily_money_cached(BATCH_CODES[:2], "2026-07-20", count=3)
    assert len(src.batch_calls) == 1
    assert src.batch_calls[0][0] == [BATCH_CODES[1]]


def test_build_money_full_falls_back_per_code_on_batch_failure(tmp_path):
    """批量失败：降级逐只 fetch，结果仍正确。"""
    src = _BatchSource({c: _fresh_dates() for c in BATCH_CODES}, fail_batch=True)
    dm = _live_dm(tmp_path, src)
    res = dm.get_daily_money_cached(BATCH_CODES, "2026-07-20", count=3)
    assert len(src.batch_calls) == 1          # 试过批量
    assert len(src.daily_calls) == len(BATCH_CODES)  # 逐只降级
    for c in BATCH_CODES:
        assert c in set(res["code"])


def test_build_money_full_without_batch_support_falls_back(tmp_path):
    """旧源不支持 get_daily_batch：直接走逐只，行为与改造前一致。"""
    src = _PerCodeOnlySource({c: _fresh_dates() for c in BATCH_CODES})
    dm = _live_dm(tmp_path, src)
    res = dm.get_daily_money_cached(BATCH_CODES, "2026-07-20", count=3)
    assert len(src.daily_calls) == len(BATCH_CODES)
    for c in BATCH_CODES:
        assert c in set(res["code"])


def test_fetch_daily_single_arg_default_window_narrowed(tmp_path):
    """fetch("get_daily", code) 单参兜底窗口：回看 400 天，而非 2000-01-01。"""
    src = _PerCodeOnlySource({CODE: _fresh_dates()})
    dm = _live_dm(tmp_path, src)
    dm.fetch("get_daily", CODE)
    assert len(src.daily_calls) == 1
    _, start, end = src.daily_calls[0]
    today = pd.Timestamp.today().normalize()
    lo = (today - pd.Timedelta(days=401)).strftime("%Y-%m-%d")
    hi = (today - pd.Timedelta(days=399)).strftime("%Y-%m-%d")
    assert lo <= start <= hi, f"start 应为回看 ~400 天: {start}"
    assert end == today.strftime("%Y-%m-%d"), f"end 应为今天: {end}"


def test_build_money_full_batch_window_narrowed(tmp_path):
    """批量请求窗口同样收窄为回看 ~400 天（不再传 2000-01-01）。"""
    src = _BatchSource({c: _fresh_dates() for c in BATCH_CODES})
    dm = _live_dm(tmp_path, src)
    dm.get_daily_money_cached(BATCH_CODES, "2026-07-20", count=3)
    assert len(src.batch_calls) == 1
    _, start, _ = src.batch_calls[0]
    today = pd.Timestamp.today().normalize()
    lo = (today - pd.Timedelta(days=401)).strftime("%Y-%m-%d")
    hi = (today - pd.Timedelta(days=399)).strftime("%Y-%m-%d")
    assert lo <= start <= hi, f"批量 start 应为回看 ~400 天: {start}"


def test_minute_diag_logs_on_today_missing(tmp_path, monkeypatch, caplog):
    """开启 _diag_minute 后，盘中取数空应输出可定位的诊断日志（不进 _minute_empty）。

    便于排查"当日分钟缺失被误判临时停牌"（08-13 159768 案例）：日志应包含
    标的名、窗口、以及"盘中取数空(未进_minute_empty)"，而非静默。
    """
    import logging
    from app.quant.jqengine.datasource import manager as mgr_mod

    today = "2026-03-25"
    monkeypatch.setattr(mgr_mod.pd.Timestamp, "now",
                        classmethod(lambda cls, tz=None: pd.Timestamp(f"{today} 12:00:00")))
    dm = _make_dm(tmp_path)
    dm._diag_minute = True
    dm.client = _DelayedMinuteClient({}, snap_frames={})  # 全部无数据
    with caplog.at_level(logging.WARNING, logger="app.quant.jqengine.datasource.manager"):
        dm._ensure_minute_windowed(CODE, today)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("盘中取数空" in m and CODE in m for m in msgs), msgs
    assert CODE not in dm._minute_empty
