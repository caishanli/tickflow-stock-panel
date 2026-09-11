"""备用日线链回归测试（全 mock，不触网）。

链序：腾讯 fqkline(day) → 新浪 K线(scale=240) → mootdx。
口径实锤：腾讯行 [日期, 开, 收, 高, 低, 量(手)]；新浪 volume=股；
股票分区 volume=手、ETF 分区=股；amount 估算。
"""
from datetime import date

import polars as pl
import pytest

from app.services import alt_daily as ad

D10 = date(2026, 9, 10)
D11 = date(2026, 9, 11)

T_ROWS = [["2026-09-10", "9.22", "9.35", "9.36", "9.19", "597743.000"],
          ["2026-09-11", "9.35", "9.26", "9.35", "9.22", "653273"]]
S_ROWS = [{"day": "2026-09-10", "open": "9.22", "high": "9.36", "low": "9.19",
           "close": "9.35", "volume": "59774300"},
          {"day": "2026-09-11", "open": "9.35", "high": "9.35", "low": "9.22",
           "close": "9.26", "volume": "65327300"}]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(ad, "_BACKOFF", (0.0, 0.0, 0.0))


def _frame(monkeypatch, t_rows, s_rows, symbol="600000.SH", divisor=100.0,
           days=(D10, D11)):
    calls = []
    monkeypatch.setattr(ad, "fetch_tencent_day",
                        lambda session, vendor, count=10: calls.append("t") or t_rows)
    monkeypatch.setattr(ad, "fetch_sina_day",
                        lambda session, vendor, count=10: calls.append("s") or s_rows)
    import requests
    df = ad.build_symbol_frame(symbol, divisor, set(days), requests.Session())
    return df, calls


def test_tencent_first_no_sina_call(monkeypatch):
    df, calls = _frame(monkeypatch, T_ROWS, S_ROWS)
    assert calls == ["t"]
    assert df.height == 2
    r = df.filter(pl.col("date") == D11).to_dicts()[0]
    # 腾讯列序 [日期, 开, 收, 高, 低]：O=9.35 C=9.26 H=9.35 L=9.22
    assert (r["open"], r["close"], r["high"], r["low"]) == (9.35, 9.26, 9.35, 9.22)
    assert r["volume"] == 653273.0  # 股票：手原样
    assert r["amount"] == pytest.approx(65327300.0 * (9.35 + 9.22 + 9.26) / 3)


def test_tencent_miss_falls_to_sina(monkeypatch):
    df, calls = _frame(monkeypatch, [], S_ROWS)
    assert calls == ["t", "s"]  # 空响应不重试，直接转次源
    assert df.height == 2
    r = df.filter(pl.col("date") == D10).to_dicts()[0]
    assert r["close"] == 9.35
    assert r["volume"] == 597743.0  # 新浪股÷100=手


def test_both_fail_returns_none(monkeypatch):
    df, calls = _frame(monkeypatch, None, None)
    assert df is None


def test_etf_volume_kept_shares(monkeypatch):
    df, _ = _frame(monkeypatch, T_ROWS, S_ROWS, symbol="510300.SH", divisor=1.0)
    r = df.filter(pl.col("date") == D11).to_dicts()[0]
    assert r["volume"] == 65327300.0  # ETF：股


def test_index_daily_fill(monkeypatch):
    """指数走同链：volume 保持股，宇宙来自 _index_universe。"""
    import app.services.mootdx_service as ms
    monkeypatch.setattr(ms, "_index_universe", lambda: ["000001.SH"])
    monkeypatch.setattr(ad, "_day_partition_symbols", lambda root, d: set())
    monkeypatch.setattr(ad, "fetch_tencent_day",
                        lambda session, vendor, count=10: T_ROWS)
    monkeypatch.setattr(ad, "fetch_sina_day", lambda *a, **k: [])
    written = []
    monkeypatch.setattr(ms, "_write_daily_partition",
                        lambda df, root: written.append(df))
    res = ad.sync_index_daily_alt([D10, D11])
    assert res["total"] == 2
    assert written[0].filter(pl.col("symbol") == "000001.SH").to_dicts()[0]["volume"] == 59774300.0


def test_prefer_sina_skips_tencent(monkeypatch):
    """prefer="sina"（审计覆盖重填用）：新浪覆盖全缺口日时不碰腾讯。"""
    calls = []
    monkeypatch.setattr(ad, "fetch_tencent_day",
                        lambda session, vendor, count=10: calls.append("t") or T_ROWS)
    monkeypatch.setattr(ad, "fetch_sina_day",
                        lambda session, vendor, count=10: calls.append("s") or S_ROWS)
    import requests
    df = ad.build_symbol_frame("600000.SH", 100.0, {D10, D11},
                               requests.Session(), prefer="sina")
    assert calls == ["s"]
    assert df.filter(pl.col("date") == D11).to_dicts()[0]["close"] == 9.26


def test_fill_gaps_policy(monkeypatch):
    import app.services.mootdx_service as ms
    from app.quant.jqengine.datasource import mootdx_breaker as mbr
    called = []
    monkeypatch.setattr(ad, "sync_stock_daily_alt",
                        lambda days: called.append(days) or {"total": 1})
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    assert ad.fill_recent_gaps_daily("stock") is None
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [D10])
    monkeypatch.setattr(mbr, "kline_allowed", lambda: True)
    assert ad.fill_recent_gaps_daily("stock") is None
    assert called == []
    monkeypatch.setattr(mbr, "kline_allowed", lambda: False)
    assert ad.fill_recent_gaps_daily("stock") == {"total": 1}
    assert called == [[D10]]


def test_fill_gaps_force_bypasses_closed_breaker(monkeypatch):
    """新进程熔断闭合时默认 no-op；force=True 显式绕过（运维手动补跑用）。"""
    import app.services.mootdx_service as ms
    from app.quant.jqengine.datasource import mootdx_breaker as mbr
    called = []
    monkeypatch.setattr(ad, "sync_etf_daily_alt",
                        lambda days: called.append(days) or {"total": 1})
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [D10])
    monkeypatch.setattr(mbr, "kline_allowed", lambda: True)  # 新进程：熔断闭合
    assert ad.fill_recent_gaps_daily("etf") is None
    assert called == []
    assert ad.fill_recent_gaps_daily("etf", force=True) == {"total": 1}
    assert called == [[D10]]


def test_fetch_error_summary_logged(monkeypatch, caplog):
    """备用源被 WAF 拦时不再静默：backfill_days 打一条聚合 warning。"""
    import logging

    import requests

    class _FailResp:
        status_code = 501

    class _FailSession:
        def get(self, *a, **k):
            e = requests.HTTPError("501 Server Error")
            e.response = _FailResp()
            raise e

    monkeypatch.setattr(ad.requests, "Session", lambda: _FailSession())
    with caplog.at_level(logging.WARNING, logger="app.services.alt_daily"):
        res = ad.backfill_days([("600000.SH", 100.0), ("000001.SZ", 100.0)],
                               [D10], progress="备用日线回源")
    assert res["ok_symbols"] == []
    assert res["frames"][D10] == []
    assert sorted(res["uncovered"]) == ["000001.SZ", "600000.SH"]
    summary = [r.message for r in caplog.records
               if "备用源请求失败" in r.message]
    assert summary, "expected one aggregated failure summary"
    assert "501" in summary[0]


def test_only_missing_never_overwrites(monkeypatch):
    import app.services.mootdx_service as ms
    monkeypatch.setattr(ms, "_stock_universe", lambda: ["600000.SH", "000001.SZ"])
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ad, "_day_partition_symbols", lambda root, d: {"600000.SH"})
    fetched = []
    monkeypatch.setattr(ad, "fetch_tencent_day",
                        lambda session, vendor, count=10: fetched.append(vendor) or T_ROWS)
    monkeypatch.setattr(ad, "fetch_sina_day", lambda *a, **k: [])
    written = []
    monkeypatch.setattr(ms, "_write_daily_partition",
                        lambda df, root: written.append(df))
    res = ad.sync_stock_daily_alt([D10, D11])
    assert fetched == ["sz000001"]  # 600000 两天都有，不碰网
    assert res["total"] == 2
    assert {f["symbol"][0] for f in written} == {"000001.SZ"}
