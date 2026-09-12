"""腾讯分钟回源回归测试（全 mock，不触网）。

背景：mootdx K 线全局返回空时（2026-09-10 起），分钟批量回源整批失败；
本模块用腾讯 mkline 回补近期缺口。口径已实锤：行格式 [时间, 开, 收, 高,
低, 量(手), {}, 换手率基点]（600000 全天 rollup vs 快照 OHLCV 对齐，量差 1 手）。

09-10 残缺案例驱动的新语义：have 判定按「当日最后 bar 是否到收盘段」，
对未覆盖 symbol 只追加其既有最后 bar 之后的行（symbol 出现 ≠ 当日完整）。
"""
import datetime as _dt
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
    # 800 根：320 只够 1.3 个交易日，09-10 案例里 T-2 缺口补不到（>800 上限封顶）
    assert "sh600000,m1,,800" in s.calls[0]
    assert tm._bars() == 800
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


def _write_minute_part(root, day, sym_ts: list[tuple[str, _dt.datetime]]):
    pdir = root / f"date={day.isoformat()}"
    pdir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [s for s, _ in sym_ts],
        "datetime": [t for _, t in sym_ts],
    }, schema_overrides={"datetime": pl.Datetime("us")}).write_parquet(pdir / "part.parquet")


def test_partition_keys_counts_and_completeness(tmp_path):
    """「当日完整」按 bar 数判定：symbol 存在但 bar 数量级短缺 → 不完整。"""
    root = tmp_path / "kline_minute"
    _write_minute_part(root, D10, [
        ("600000.SH", _dt.datetime(2026, 9, 10, 13, 42)),
        ("600000.SH", _dt.datetime(2026, 9, 10, 15, 0)),   # 尾段残缺（09-10 案例）
        ("000001.SZ", _dt.datetime(2026, 9, 10, 9, 31)),
        ("000001.SZ", _dt.datetime(2026, 9, 10, 15, 0)),
    ])
    keys = tm._partition_keys(root, D10)
    assert keys is not None and keys.height == 4
    counts = tm._bar_counts(keys)
    assert counts == {"600000.SH": 2, "000001.SZ": 2}
    assert not tm._day_complete(counts, "600000.SH")   # 2 根 << 200
    assert not tm._day_complete(counts, "缺 partition.SH")
    # 完整日（241 根）才判完整
    full = dict.fromkeys(["600000.SH"], 241)
    assert tm._day_complete(full, "600000.SH")
    assert tm._partition_keys(root, D11) is None  # 缺分区 → None


def test_fill_recent_gaps_policy(monkeypatch):
    import app.services.mootdx_service as ms
    from app.quant.jqengine.datasource import mootdx_breaker as mbr
    called = []
    monkeypatch.setattr(tm, "sync_stock_minute_tencent",
                        lambda days, **k: called.append(days) or {"total": 1})
    monkeypatch.setattr(ms, "_short_bar_minute_days", lambda root, **k: [])
    # 无缺口 → 不触发
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda: [])
    assert tm.fill_recent_gaps("stock") is None
    assert called == []
    # 分区齐全但 bar 数量级残缺（09-10 案例：symbol 覆盖 99.8%，每只仅 79 根）
    # → 同样视为缺口触发回补
    monkeypatch.setattr(ms, "_short_bar_minute_days", lambda root, **k: [D10])
    monkeypatch.setattr(mbr, "kline_allowed", lambda: False)
    assert tm.fill_recent_gaps("stock") == {"total": 1}
    assert called == [[D10]]
    called.clear()
    # 有缺口但熔断关闭 → 不触发（mootdx 自行处理）
    monkeypatch.setattr(ms, "_short_bar_minute_days", lambda root, **k: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda: [D10])
    monkeypatch.setattr(mbr, "kline_allowed", lambda: True)
    assert tm.fill_recent_gaps("stock") is None
    assert called == []
    # 有缺口 + 开路 → 触发
    monkeypatch.setattr(mbr, "kline_allowed", lambda: False)
    assert tm.fill_recent_gaps("stock") == {"total": 1}
    assert called == [[D10]]


def test_only_missing_fills_holes_never_overwrites(monkeypatch, tmp_path):
    """缺口按 (symbol,datetime) 反连接填充：洞在上午/尾段都能补，已有 bar 不动。"""
    import app.services.mootdx_service as ms
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: ["600001.SH", "600000.SH", "000001.SZ"])
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    # 分区现状（模拟 09-10 残缺形态：尾段已有、上午缺失）：
    # - 600001.SH 两天都完整（241 根）→ 整批跳过，连网都不碰
    # - 600000.SH D10 只有尾段 2 根、D11 完整 → 拉取后只补 D10 缺失时间戳
    # - 000001.SZ 两天分区都没有 → 全量补
    root = tmp_path / "sm"
    _write_minute_part(root, D10, [
        ("600001.SH", _dt.datetime(2026, 9, 10, 9, 31)),
        ("600001.SH", _dt.datetime(2026, 9, 10, 15, 0)),
        ("600000.SH", _dt.datetime(2026, 9, 10, 13, 42)),   # 尾段残缺
        ("600000.SH", _dt.datetime(2026, 9, 10, 15, 0)),
    ])
    # 600001 补足 241 根（bar 数 ≥200 判完整）
    pdir = root / f"date={D10.isoformat()}"
    df = pl.read_parquet(pdir / "part.parquet")
    extra = pl.DataFrame({
        "symbol": ["600001.SH"] * 239,
        "datetime": [_dt.datetime(2026, 9, 10, 9, 32) + _dt.timedelta(minutes=i)
                     for i in range(239)],
    }, schema_overrides={"datetime": pl.Datetime("us")})
    pl.concat([df, extra]).write_parquet(pdir / "part.parquet")
    _write_minute_part(root, D11, [
        ("600001.SH", _dt.datetime(2026, 9, 11, 15, 0)),
        ("600000.SH", _dt.datetime(2026, 9, 11, 15, 0)),
        ("600000.SH", _dt.datetime(2026, 9, 11, 9, 31)),
    ])
    pdir11 = root / f"date={D11.isoformat()}"
    df11 = pl.read_parquet(pdir11 / "part.parquet")
    extra11 = pl.DataFrame({
        "symbol": ["600001.SH"] * 240,
        "datetime": [_dt.datetime(2026, 9, 11, 9, 31) + _dt.timedelta(minutes=i)
                     for i in range(240)],
    }, schema_overrides={"datetime": pl.Datetime("us")})
    pl.concat([df11, extra11]).write_parquet(pdir11 / "part.parquet")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    fetched = []
    frames_600 = tm.parse_m1_rows("600000.SH", [
        ["202609100931", "9.3", "9.3", "9.3", "9.3", "100", {}, "0"],   # 缺 → 补
        ["202609101342", "9.3", "9.3", "9.3", "9.3", "100", {}, "0"],   # 已有 → 反连接剔除
        ["202609101500", "9.3", "9.3", "9.3", "9.3", "100", {}, "0"],   # 已有 → 反连接剔除
        ["202609111500", "9.3", "9.3", "9.3", "9.3", "100", {}, "0"],   # D11 已有 → 剔除
    ], {D10, D11})
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
    assert fetched == ["600000.SH", "000001.SZ"]  # 600001 两天完整，连网都不碰
    # D10: 600000 仅补缺失的 09:31 一根 + 000001 一根全补（ROWS 中 D10 仅 15:00）
    # D11: 600000 的 15:00 已有 → 反连接后为空，只剩 000001 两根
    assert res["rows"] == {D10.isoformat(): 2, D11.isoformat(): 2}
    assert res["total"] == 4
    by_day: dict = {}
    for f in flushed:
        # 反连接后的 combined 帧是多 symbol 的，须逐行统计
        for sym, t in zip(f["symbol"].to_list(), f["datetime"].to_list(),
                          strict=True):
            by_day.setdefault(sym, []).append(t)
    assert by_day["600000.SH"] == [_dt.datetime(2026, 9, 10, 9, 31)]
    assert set(by_day["000001.SZ"]) == set(frames_001["datetime"].to_list())
