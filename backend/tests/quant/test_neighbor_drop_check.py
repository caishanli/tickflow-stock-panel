"""邻居相对检测（``_neighbor_drop_partition_days``）回归。

2026-09-12 全库审计：北交所段缺失日 5215/5550≈93.9% 覆盖，绝对覆盖率
（0.5 与 0.95）都测不出；而绝对 0.95 会把 ETF 扩容史（每日 +3~4 只）的
~180 天全误判。邻居相对（<95% 邻居中位行数）两头都兜住。
"""
from __future__ import annotations

from datetime import date

import polars as pl

from app.services import mootdx_service as ms


def _write_day(root, day: str, n: int) -> None:
    pdir = root / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"{600000 + i}.SH" for i in range(n)],
    }).write_parquet(pdir / "part.parquet")


def test_segment_hole_flagged(tmp_path):
    """段缺失日（93.9% 邻居中位）被检出，邻居间正常波动不误报。"""
    root = tmp_path / "kline_daily"
    rows = [5550, 5549, 5215, 5557, 5216, 5550]  # 第3/5天缺 342 只
    for i, n in enumerate(rows):
        _write_day(root, f"2026-09-0{i+1}", n)
    out = ms._neighbor_drop_partition_days(root, recent=10)
    assert out == [date(2026, 9, 3), date(2026, 9, 5)]


def test_gradual_expansion_not_flagged(tmp_path):
    """ETF 扩容形态：每日 +3~4 只，绝不会被邻居比对误报。"""
    root = tmp_path / "kline_etf_daily"
    for i, n in enumerate(range(1576, 1600, 4)):
        _write_day(root, f"2026-05-{i+10:02d}", n)
    assert ms._neighbor_drop_partition_days(root, recent=30) == []


def test_all_short_days_flagged(tmp_path):
    """整段都短时以邻居中位兜底仍能检出（4/600 残帧日形态）。"""
    root = tmp_path / "kline_index_daily"
    for i in range(5):
        _write_day(root, f"2026-07-2{i+5}", 4 if i == 2 else 598)
    assert ms._neighbor_drop_partition_days(root, recent=10) == [date(2026, 7, 27)]


def test_intraday_today_skipped(tmp_path, monkeypatch):
    root = tmp_path / "kline_daily"
    _write_day(root, "2026-09-09", 5550)
    _write_day(root, date.today().isoformat(), 100)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: False)
    assert ms._neighbor_drop_partition_days(root, recent=10) == []
