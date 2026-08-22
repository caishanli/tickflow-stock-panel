"""mootdx_service.sync_adj_factor 因子计算单元测试.

覆盖:
- cat==1 (除权除息) 含现金红利/配股: 因子 = (prev_close - fh + pgj*pg)
  / (1+sg+pg) / prev_close, 与 mootdx_src._to_qfq 同口径 (对齐 bug:
  旧实现 1/(1+sg+pg) 漏掉现金红利 fh, 510880 等年度分红标的复权价偏离).
- cat==11 (扩缩股): factor = 1/suogu (159667 拆分 3x -> 1/3).
"""
from collections import OrderedDict

import pandas as pd
import pytest

from app.services import mootdx_service as ms

# ---------------------------------------------------------------------------
# 构造 xdxr 记录(OrderedDict,与 mootdx 返回结构一致)
# ---------------------------------------------------------------------------


def _xdxr(cat, year, month, day, fenhong=None, peigujia=None,
          songzhuangu=None, peigu=None, suogu=None):
    return OrderedDict([
        ("year", year), ("month", month), ("day", day), ("category", cat),
        ("name", "x"), ("fenhong", fenhong), ("peigujia", peigujia),
        ("songzhuangu", songzhuangu), ("peigu", peigu), ("suogu", suogu),
    ])


def _fake_src(events_by_sym):
    class _FakeSrc:
        def _xdxr_rows(self, sym):
            # sync_adj_factor 传 6 位纯代码,映射回带后缀的 key
            return events_by_sym.get(
                next((k for k in events_by_sym if k.split(".")[0] == sym), ""))

    return _FakeSrc()


def _run_sync_adj_factor(monkeypatch, events_by_sym, daily_map, codes):
    monkeypatch.setattr(ms, "_etf_universe", lambda: codes)

    class _FakeDM:
        def _load_daily_from_partitions(self, asof=None):
            return daily_map

    monkeypatch.setattr(ms, "DataManager", lambda: _FakeDM())
    monkeypatch.setattr(ms, "MootdxSource", lambda: _fake_src(events_by_sym))
    return ms.sync_adj_factor()


def _factor_table(result, sym):
    """从 sync_adj_factor 结果文件里取某标的的 (date_str -> ex_factor)."""
    import polars as pl
    df = pl.read_parquet(str(ms.ADJ_FACTOR_PATH))
    df = df.filter(pl.col("symbol") == sym).sort("trade_date")
    out = {}
    for r in df.iter_rows():
        d = r[1]  # (symbol, trade_date, ex_factor)
        key = d.isoformat() if hasattr(d, "isoformat") else str(d)
        out[key] = r[2]
    return out


# ---------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------

def test_cat11_suogu_factor(monkeypatch, tmp_path):
    """扩缩股(cat==11):159667 拆分 3x -> 因子 1/3."""
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH",
                        tmp_path / "adj_factor_etf" / "all.parquet")
    sym = "159667.XSHE"
    days = pd.date_range("2026-05-25", "2026-06-15", freq="B")
    closes = [1.0] * len(days)  # 简化为常数,验证因子是 1/3
    daily = {sym: pd.DataFrame({"close": closes}, index=pd.to_datetime(days))}
    ev = {sym: [_xdxr(11, 2026, 6, 10, suogu=3.0)]}
    result = _run_sync_adj_factor(monkeypatch, ev, daily, [sym])
    assert result["written_symbols"] == 1, f"written_symbols={result}"
    tab = _factor_table(result, sym)
    print("factor table keys:", sorted(tab.keys()))
    # 6/10 前因子 = 1/3,6/10 后 = 1.0
    assert tab["2026-06-09"] == pytest.approx(1 / 3)
    assert tab["2026-06-10"] == pytest.approx(1.0)


def test_cat1_cash_dividend_factor(monkeypatch, tmp_path):
    """现金分红(cat==1, fenhong>0, sg=pg=0):因子含分红摊薄."""
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH",
                        tmp_path / "adj_factor_etf" / "all.parquet")
    sym = "510880.XSHE"
    days = ["2026-01-19", "2026-01-20", "2026-01-21", "2026-01-22"]
    closes = [2.0, 2.0, 2.0, 2.0]  # prev_close(除权日前)=2.0
    daily = {sym: pd.DataFrame({"close": closes}, index=pd.to_datetime(days))}
    # fenhong=1.43(元/10股) -> 每股 0.143;除权日 1/21
    ev = {sym: [_xdxr(1, 2026, 1, 21, fenhong=1.43)]}
    result = _run_sync_adj_factor(monkeypatch, ev, daily, [sym])
    tab = _factor_table(result, sym)
    fh = 1.43 / 10.0
    expected = (2.0 - fh) / 2.0  # (prev - fh)/prev
    assert tab["2026-01-20"] == pytest.approx(expected)
    assert tab["2026-01-21"] == pytest.approx(1.0)


def test_cat1_no_dividend_no_send_skipped(monkeypatch, tmp_path):
    """无分红无送转(fh=sg=pg=0):无除权事件,不写因子表."""
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH",
                        tmp_path / "adj_factor_etf" / "all.parquet")
    sym = "510300.XSHE"
    days = ["2026-01-20", "2026-01-21", "2026-01-22"]
    closes = [2.0, 2.0, 2.0]
    daily = {sym: pd.DataFrame({"close": closes}, index=pd.to_datetime(days))}
    ev = {sym: [_xdxr(1, 2026, 1, 21, fenhong=0.0)]}
    result = _run_sync_adj_factor(monkeypatch, ev, daily, [sym])
    assert result["written_symbols"] == 0


# ---------------------------------------------------------------------------
# 三层加固：原始事件落本地 / 查询失败不缓存不丢标的 / 断点审计兜底
# ---------------------------------------------------------------------------

def _mk_daily(sym="159667.XSHE", pre=3.0, post=1.0):
    days = pd.date_range("2026-05-25", "2026-06-15", freq="B")
    n_pre = sum(1 for d in days if d < pd.Timestamp("2026-06-10"))
    closes = [pre] * n_pre + [post] * (len(days) - n_pre)
    return {sym: pd.DataFrame({"close": closes}, index=pd.to_datetime(days))}, days


def test_raw_events_persisted(monkeypatch, tmp_path):
    """第2层：xdxr 原始事件行落 xdxr_events.parquet。"""
    import polars as pl
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj" / "all.parquet")
    sym = "159667.XSHE"
    daily, _ = _mk_daily(sym)
    _run_sync_adj_factor(monkeypatch, {sym: [_xdxr(11, 2026, 6, 10, suogu=3.0)]},
                         daily, [sym])
    ev_path = tmp_path / "adj" / "xdxr_events.parquet"
    assert ev_path.exists(), "事件表未落盘"
    df = pl.read_parquet(ev_path)
    row = df.filter(pl.col("symbol") == "159667").row(0, named=True)
    assert row["category"] == 11 and abs(row["suogu"] - 3.0) < 1e-9


def test_query_failure_keeps_local_events(monkeypatch, tmp_path):
    """第2层：次轮查询全失败 → 沿用本地事件重建，因子不丢。"""
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj" / "all.parquet")
    sym = "159667.XSHE"
    daily, _ = _mk_daily(sym)
    _run_sync_adj_factor(monkeypatch, {sym: [_xdxr(11, 2026, 6, 10, suogu=3.0)]},
                         daily, [sym])

    class _DeadSrc:
        def _xdxr_rows(self, s):
            return None

    monkeypatch.setattr(ms, "MootdxSource", lambda: _DeadSrc())
    result = ms.sync_adj_factor()
    assert result["query_failed"] == ["159667"]
    tab = _factor_table(result, sym)
    assert any(abs(v - 1 / 3) < 1e-6 for v in tab.values()), "本地事件应保住拆分因子"


def test_xdxr_failure_not_cached(monkeypatch):
    """第1层：整轮轮换失败返回 None 且不缓存，下次调用重试。"""
    from app.quant.jqengine.datasource.mootdx_src import MootdxSource
    src = MootdxSource.__new__(MootdxSource)
    src._xdxr_cache = {}
    calls = {"n": 0}

    def fake_retry(fn, empty_ok=False):
        calls["n"] += 1
        return (None, "down") if calls["n"] == 1 else ([], None)

    monkeypatch.setattr(src, "_with_server_retry", fake_retry)
    assert src._xdxr_rows("159667") is None
    assert src._xdxr_rows("159667") == []
    assert calls["n"] == 2


def test_audit_retries_and_warns(monkeypatch, tmp_path, caplog):
    """第3层：首轮源漏事件 → 审计发现断点 → 重试轮补回 → 因子齐；仍缺则告警。"""
    import logging
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj" / "all.parquet")
    sym = "159667.XSHE"
    daily, _ = _mk_daily(sym)
    # 隔离真实数据：宇宙只含测试标的，日线用构造帧（否则审计会扫到
    # 本地全市场分区里真实的大幅波动，误报断点缺口）
    monkeypatch.setattr(ms, "_etf_universe", lambda: [sym])

    class _FakeDM:
        def _load_daily_from_partitions(self, asof=None):
            return daily

    monkeypatch.setattr(ms, "DataManager", lambda: _FakeDM())

    class _SeqSrc:
        insts = []

        def __init__(self):
            self.n = 0
            _SeqSrc.insts.append(self)

        def _xdxr_rows(self, s):
            self.n += 1
            # 第一个实例（首轮）返回空；重试轮实例给出事件
            return [] if len(_SeqSrc.insts) == 1 else [_xdxr(11, 2026, 6, 10, suogu=3.0)]

    monkeypatch.setattr(ms, "MootdxSource", _SeqSrc)
    with caplog.at_level(logging.WARNING, logger="mootdx_service"):
        result = ms.sync_adj_factor()
    tab = _factor_table(result, sym)
    assert any(abs(v - 1 / 3) < 1e-6 for v in tab.values()), "重试轮应补回拆分因子"
    assert result["audit_uncovered"] == [], "重试后不应再有未覆盖断点"


def test_sync_daily_query_failed_and_retry(monkeypatch):
    """日线链路：首轮查询失败（守护折叠为 None）→ 计入失败并当轮重试成功。"""
    from datetime import date as _d
    from app.services import mootdx_service as ms
    calls = {"n": 0}

    class _FlakySrc:
        def get_daily(self, code, start, end):
            calls["n"] += 1
            if calls["n"] == 1:
                raise TimeoutError("socket down")
            import pandas as pd
            return pd.DataFrame(
                {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
                 "volume": [100.0], "amount": [100.0]},
                index=pd.to_datetime(["2026-08-20 15:00"]))

    monkeypatch.setattr(ms, "_stock_universe", lambda: ["600000.XSHG"])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [])
    monkeypatch.setattr(ms, "MootdxSource", _FlakySrc)
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ms, "_throttle_backfill", lambda i: None)
    monkeypatch.setattr(ms, "_write_daily_partition", lambda df, root: None)
    res = ms.sync_daily(_d(2026, 8, 20))
    assert calls["n"] == 2, f"失败标的应被重试: {calls}"
    assert res["stock"] == 1
    assert res["query_failed"] == []


def test_sync_etf_minute_retry_round(monkeypatch):
    """ETF 分钟链路：首轮失败标的进 query_failed，重试轮成功后正常落盘。"""
    import pandas as pd
    from app.services import mootdx_service as ms
    calls = {"n": 0}

    class _Src:
        def get_minute_recent(self, jq, pages=2):
            calls["n"] += 1
            if calls["n"] == 1:
                raise TimeoutError("down")
            out = pd.DataFrame(
                {"close": [1.0], "volume": [10], "amount": [10.0],
                 "open": [1.0], "high": [1.0], "low": [1.0]},
                index=pd.to_datetime(["2026-08-20 09:31:00"]))
            # mootdx 真实帧的 DatetimeIndex 名为 datetime，reset_index 后列名才是 datetime
            out.index.name = "datetime"
            return out

    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159667.XSHE"])
    monkeypatch.setattr(ms, "MootdxSource", _Src)
    monkeypatch.setattr(ms, "_throttle_backfill", lambda i: None)
    monkeypatch.setattr(ms, "_write_minute_partition", lambda df, root, day=None: df.height)
    from datetime import date as _d
    res = ms.sync_etf_minute(_d(2026, 8, 20))
    assert calls["n"] == 2, "失败标的应被重试"
    assert res["rows"] == 1 and res["query_failed"] == []


def test_sync_stock_minute_retry_round(monkeypatch):
    """股票分钟链路：首轮失败→重试轮成功，query_failed 收敛为空。"""
    import pandas as pd
    from datetime import date as _d
    from app.services import mootdx_service as ms
    calls = {"n": 0}

    class _Src:
        def get_minute(self, sym, max_bars=40000):
            calls["n"] += 1
            if calls["n"] == 1:
                raise TimeoutError("down")
            out = pd.DataFrame(
                {"close": [1.0, 1.1], "volume": [10, 20], "amount": [10.0, 22.0],
                 "open": [1.0, 1.0], "high": [1.1, 1.1], "low": [1.0, 1.0]},
                index=pd.to_datetime(["2026-08-19 15:00", "2026-08-20 15:00"]))
            out.index.name = "datetime"
            return out

    monkeypatch.setattr(ms, "_stock_universe", lambda: ["600000.XSHG"])
    monkeypatch.setattr(ms, "_existing_minute_symbols", lambda: set())
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_minute_fragment_days", lambda: {})
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ms, "_market_closed", lambda: True)
    monkeypatch.setattr(ms, "_flush_stock_minute_chunk", lambda chunk: None)
    monkeypatch.setattr(ms, "_throttle_backfill", lambda i: None)
    monkeypatch.setattr(ms, "_guarded_get_minute",
                        lambda src, sym, max_bars=40000: src.get_minute(sym, max_bars=max_bars))
    monkeypatch.setattr(ms, "MootdxSource", _Src)
    res = ms.sync_stock_minute(limit=None)
    assert calls["n"] == 2, "失败标的应被重试"
    assert res["rows"] == 2 and res["query_failed"] == []


def test_put_daily_mem_enforces_money_column(monkeypatch):
    """防御纵深：任何写入方往 _daily_mem 塞无 money 日线帧（如网络批量
    返回的服务端原始列），入口统一补齐 money/volume——防池过滤/阈值计算
    被 'Column not found: money' 打断。"""
    import pandas as pd
    from app.quant.jqengine.datasource.manager import DataManager
    dm = DataManager.__new__(DataManager)
    dm._daily_mem = {}
    dm._daily_ver = 0
    dm._offline = False
    raw = pd.DataFrame(
        {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
         "volume": [100.0], "amount": [12345.0],
         "trade_dt": pd.to_datetime(["2026-08-20"]).date},
        index=pd.to_datetime(["2026-08-20"]))
    dm._put_daily_mem_protected("get_daily_510300.XSHG", raw)
    saved = dm._daily_mem["get_daily_510300.XSHG"]
    assert "money" in saved.columns
    assert float(saved["money"].iloc[0]) == 12345.0
