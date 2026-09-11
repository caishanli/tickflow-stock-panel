"""腾讯分钟回源回归测试（全 mock，不触网）。

背景：mootdx K 线全局返回空时（2026-09-10 起），分钟批量回源整批失败；
本模块用腾讯 mkline 回补近期缺口。口径已实锤：行格式 [时间, 开, 收, 高,
低, 量(手), {}, 换手率基点]（600000 全天 rollup vs 快照 OHLCV 对齐，量差 1 手）。
"""
from datetime import date

import polars as pl
import pytest

from app.services import tencent_minute as tm

D10 = date(2026, 9, 10)
D11 = date(2026, 9, 11)

ROWS = [
    ["202609101500", "9.20", "9.22", "9.23", "9.20", "1000.00", {}, "0.03"],
    ["202609110930", "9.35", "9.35", "9.35", "9.35", "3778.00", {}, "0.11"],
    ["202609111500", "9.25", "9.26", "9.26", "9.25", "8473.00", {}, "0.25"],
    ["bad-row"],
    ["202609111501", "0", "0", "0", "0", "10.00", {}, "0"],  # 零价丢弃
]


def test_tf_to_vendor():
    assert tm.tf_to_vendor("600000.SH") == "sh600000"
    assert tm.tf_to_vendor("000001.SZ") == "sz000001"
    assert tm.tf_to_vendor("920001.BJ") is None
    assert tm.tf_to_vendor("oops") is None


def test_parse_field_order_and_units():
    df = tm.parse_m1_rows("600000.SH", ROWS, {D10, D11})
    assert df.height == 3
    r930 = df.filter(pl.col("datetime").dt.strftime("%H%M") == "0930").to_dicts()[0]
    # [时间, 开, 收, 高, 低]：开=收=高=低=9.35
    assert (r930["open"], r930["close"], r930["high"], r930["low"]) == (9.35,) * 4
    assert r930["volume"] == 377800.0  # 手×100=股
    r1500 = df.filter(pl.col("datetime") == pl.datetime(2026, 9, 11, 15, 0)).to_dicts()[0]
    # 1500 bar 验证 开/收/高/低 不错位：[9.25, 9.26, 9.26, 9.25]
    assert (r1500["open"], r1500["close"], r1500["high"], r1500["low"]) == (9.25, 9.26, 9.26, 9.25)
    # amount = volume × typical((H+L+C)/3)
    assert r1500["amount"] == pytest.approx(847300.0 * (9.26 + 9.25 + 9.26) / 3)


def test_parse_day_filter_and_schema():
    df = tm.parse_m1_rows("600000.SH", ROWS, {D11})
    assert df.height == 2
    assert set(df["datetime"].dt.date().to_list()) == {D11}
    empty = tm.parse_m1_rows("600000.SH", [], {D11})
    assert empty.is_empty()
    assert empty.schema["volume"] == pl.Float64


class _FakeResp:
    def __init__(self, payload=None, boom=False):
        self._payload = payload
        self._boom = boom

    def raise_for_status(self):
        if self._boom:
            raise OSError("net down")

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _FakeSession:
    def __init__(self, payloads):
        self._payloads = list(payloads)
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        p = self._payloads.pop(0) if self._payloads else []
        if isinstance(p, Exception):
            raise p
        return _FakeResp(p)


def _mk_payload(rows):
    return {"code": 0, "data": {"sh600000": {"m1": rows}}}


def test_fetch_m1_ok_and_shapes():
    s = _FakeSession([_mk_payload(ROWS)])
    assert tm.fetch_m1(s, "sh600000") == ROWS
    assert "sh600000,m1,,320" in s.calls[0]
    # 空 m1（停牌/限流）→ 重试后返回空帧（每次均空，非网络失败）
    assert tm._fetch_symbol(
        _FakeSession([_mk_payload([])] * 3), "600000.SH", {D11}).is_empty()
    # HTTP 失败 → _fetch_symbol 重试耗尽返回 None
    assert tm._fetch_symbol(_FakeSession([Exception("x")] * 5), "600000.SH", {D11}) is None


def test_fetch_symbol_retry_then_ok(monkeypatch):
    # 空响应（WAF 限流）不重试、当场返回空帧；仅网络失败退避重试
    df = tm._fetch_symbol(_FakeSession([_mk_payload([])]),
                          "600000.SH", {D11}, )
    assert df.is_empty()
    import requests as _rq
    calls = []
    orig = tm.fetch_m1

    def _flaky(session, vendor):
        calls.append(1)
        if len(calls) < 3:
            return None
        return orig(session, vendor)

    monkeypatch.setattr(tm, "fetch_m1", _flaky)
    monkeypatch.setattr(tm, "_BACKOFF", (0.0, 0.0, 0.0))
    s = _FakeSession([_mk_payload(ROWS)])
    df = tm._fetch_symbol(s, "600000.SH", {D11})
    assert df.height == 2
    assert len(calls) == 3 and len(s.calls) == 1


def test_fill_recent_gaps_policy(monkeypatch):
    import app.services.mootdx_service as ms
    from app.quant.jqengine.datasource import mootdx_breaker as mbr
    called = []
    monkeypatch.setattr(tm, "sync_stock_minute_tencent",
                        lambda days, **k: called.append(days) or {"total": 1})
    # 无缺口 → 不触发
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda: [])
    assert tm.fill_recent_gaps("stock") is None
    assert called == []
    # 有缺口但熔断关闭 → 不触发（mootdx 自行处理）
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda: [D10])
    monkeypatch.setattr(mbr, "kline_allowed", lambda: True)
    assert tm.fill_recent_gaps("stock") is None
    assert called == []
    # 有缺口 + 开路 → 触发
    monkeypatch.setattr(mbr, "kline_allowed", lambda: False)
    assert tm.fill_recent_gaps("stock") == {"total": 1}
    assert called == [[D10]]


def test_only_missing_never_overwrites(monkeypatch):
    import app.services.mootdx_service as ms
    monkeypatch.setattr(ms, "_stock_universe", lambda: ["600000.SH", "000001.SZ"])
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    # 600000 两天分区里都有 → 整批跳过；000001 两天都没有 → 全拉
    monkeypatch.setattr(tm, "_partition_symbols",
                        lambda root, d: {"600000.SH"})
    fetched = []
    frames_600 = tm.parse_m1_rows("600000.SH", ROWS, {D10, D11})
    frames_001 = tm.parse_m1_rows("000001.SZ", ROWS, {D10, D11})

    def _fake_backfill(syms, days, progress=""):
        fetched.extend(syms)
        frames = {}
        for d in days:
            day_frames = []
            for f, sym in ((frames_600, "600000.SH"), (frames_001, "000001.SZ")):
                if sym in syms:
                    sub = f.filter(pl.col("datetime").dt.date() == d)
                    if not sub.is_empty():
                        day_frames.append(sub)
            frames[d] = day_frames
        return {"frames": frames, "ok_symbols": list(syms), "uncovered": []}

    monkeypatch.setattr(tm, "backfill_days", _fake_backfill)
    flushed = []
    monkeypatch.setattr(ms, "_flush_stock_minute_chunk", flushed.extend)
    res = tm.sync_stock_minute_tencent([D10, D11])
    assert fetched == ["000001.SZ"]  # 600000 两天都有，连网都不碰
    # D10/D11: 仅000001（各1行/2行，600000被过滤，mootdx bar 保留）
    assert res["total"] == 3
    assert res["rows"] == {D10.isoformat(): 1, D11.isoformat(): 2}
    assert {f["symbol"][0] for f in flushed} == {"000001.SZ"}
