"""_short_bar_minute_days：分钟分区「bar 数量级残缺」日检测回归测试。

背景（2026-09-10 案例）：mootdx 上午 10:49 熔断 + 腾讯 320 根回补只够到
10:49，9.10 分区含全部 symbol（覆盖率 99.8%）但每只仅 ~79 根——symbol
覆盖率类检测（_incomplete_partition_days/_shortfall_days）对此全盲，残缺
日永久搁浅。本检测按「bar 数 < 阈值的 symbol 占比」判定。
"""
from __future__ import annotations

import datetime as _dt

import polars as pl

from app.services import mootdx_service as ms

D10 = _dt.date(2026, 9, 10)
D11 = _dt.date(2026, 9, 11)


def _write_part(root, day, counts: dict[str, int], bar_dt=_dt.time(10, 0)):
    """写一个 date= 分区：每 symbol ``counts[sym]`` 根 bar（时间错开）。"""
    pdir = root / f"date={day.isoformat()}"
    pdir.mkdir(parents=True, exist_ok=True)
    syms, ts = [], []
    for sym, n in counts.items():
        for j in range(n):
            syms.append(sym)
            ts.append(_dt.datetime.combine(day, bar_dt) + _dt.timedelta(minutes=j))
    pl.DataFrame({
        "symbol": syms,
        "datetime": ts,
    }, schema_overrides={"datetime": pl.Datetime("us")}).write_parquet(pdir / "part.parquet")


def test_short_bar_day_detected(tmp_path, monkeypatch):
    """全市场 symbol 都只 79 根 → 判残缺；正常 241 根日 → 不判。"""
    root = tmp_path / "kline_minute"
    _write_part(root, D10, {f"{600000 + i}.SH": 79 for i in range(100)})
    _write_part(root, D11, {f"{600000 + i}.SH": 241 for i in range(100)})
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    out = ms._short_bar_minute_days(root, lookback=10, bar_min=200, fraction=0.01)
    assert out == [D10]


def test_suspended_minority_not_flagged(tmp_path, monkeypatch):
    """少数停牌/一字板（<200 根）属合法短缺，不超过 5% 不误判。

    ETF 实测正常日也有 ~1% 停牌/半日标的（09-11：17/1658 <200），阈值取
    5% 才不误报（误报会让健康日被巡检反复全量重写）。
    """
    root = tmp_path / "kline_minute"
    counts = {f"{600000 + i}.SH": 241 for i in range(100)}
    for j in range(5):  # 5/105 ≈ 4.8% < 5%
        counts[f"{500000 + j}.SH"] = 180
    _write_part(root, D10, counts)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    assert ms._short_bar_minute_days(root, lookback=10, bar_min=200,
                                     fraction=0.05) == []
    # 超过 5% → 判残缺
    assert ms._short_bar_minute_days(root, lookback=10, bar_min=200,
                                     fraction=0.04) == [D10]


def test_today_intraday_skipped(tmp_path, monkeypatch):
    """盘中当日分区必然半程，不判（与既有守卫同口径）。"""
    today = _dt.date.today()
    root = tmp_path / "kline_minute"
    _write_part(root, today, {f"{600000 + i}.SH": 79 for i in range(100)})
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: False)
    assert ms._short_bar_minute_days(root, lookback=10, bar_min=200,
                                     fraction=0.01) == []


def test_empty_and_missing_partitions_ignored(tmp_path, monkeypatch):
    """目录缺失 / 分区无 parquet → 不判（归 _missing_*_days 管）。"""
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    assert ms._short_bar_minute_days(tmp_path / "nope") == []
    (tmp_path / "empty_root" / "date=2026-09-10").mkdir(parents=True)
    assert ms._short_bar_minute_days(tmp_path / "empty_root") == []
