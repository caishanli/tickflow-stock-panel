"""增强表前复权的回归测试（2026-10-30 数据审计修复）。

背景：三条链路曾同时出问题——
1. `indicators/pipeline.py` 只读 `data/adj_factor/all.parquet`，而该目录为空，
   股票因子权威源在 `adj_factor_baostock/`，导致复权被**静默跳过**：
   `kline_daily_enriched` 全部 1,130 万行 `close == raw_close`。
2. 即便读到 baostock 表，`_apply_adj_factor` 把**累计后复权因子**当逐次事件
   比值做 cumprod，实测把 raw 312.40 复权成 400,551。
3. 修好复权后暴露第三个问题：涨跌停判定在除权日拿 `raw_close` 去比用**复权
   昨收**算出的涨跌停价，基准不一致，产生 2,653 处虚假涨停。

本测试锁住 (1)(2) 的修复；第 (3) 点由 `_cmp_close` 在 pipeline 内部处理。
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from app.indicators.pipeline import (
    _apply_adj_factor,
    _cumulative_to_events,
    _load_factors,
    _resolve_factor_source,
)

# 300628.SZ 的真实 baostock 累计后复权因子（节选）
_CUM_300628 = [
    ("300628.SZ", dt.date(2016, 1, 1), 0.047066),
    ("300628.SZ", dt.date(2017, 5, 12), 0.047329),
    ("300628.SZ", dt.date(2017, 9, 29), 0.094657),
    ("300628.SZ", dt.date(2018, 5, 28), 0.191430),
    ("300628.SZ", dt.date(2026, 9, 21), 1.000000),
]


def _cum_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in _CUM_300628],
            "trade_date": [r[1] for r in _CUM_300628],
            "ex_factor": [r[2] for r in _CUM_300628],
        }
    )


def test_cumulative_to_events_ratio_is_f_curr_over_f_prev():
    """事件比值 m_i = f_i / f_{i-1}（首行按 f_0 / 1.0）。"""
    ev = _cumulative_to_events(_cum_frame()).sort("trade_date")
    got = {r["trade_date"]: r["ex_factor"] for r in ev.to_dicts()}

    assert got[dt.date(2016, 1, 1)] == 0.047066  # 首行按 f_0/1.0
    assert abs(got[dt.date(2017, 9, 29)] - 0.094657 / 0.047329) < 1e-9
    assert abs(got[dt.date(2018, 5, 28)] - 0.191430 / 0.094657) < 1e-9
    # 相邻因子几乎不变的 2017-05-12 变化 0.56% > 1e-4 阈值，应保留
    assert dt.date(2017, 5, 12) in got


def test_cumulative_to_events_drops_negligible_moves():
    """|m-1| <= 1e-4 的事件应被丢弃（与 DataManager 噪声阈值一致）。"""
    df = pl.DataFrame(
        {
            "symbol": ["X.SZ", "X.SZ"],
            "trade_date": [dt.date(2020, 1, 1), dt.date(2020, 6, 1)],
            "ex_factor": [0.5, 0.5 * (1 + 5e-5)],
        }
    )
    ev = _cumulative_to_events(df)
    assert ev.height == 1  # 第二次变化在阈值内被丢弃


def test_apply_adj_factor_matches_manual_qfq():
    """累计表 → 事件 → _apply_adj_factor，应还原 f(d)/f_latest 的比例。

    300628.SZ 2017-05-12 raw close = 312.40，正确前复权 ≈ 14.79。
    """
    events = _cumulative_to_events(_cum_frame())
    raw = pl.DataFrame(
        {
            "symbol": ["300628.SZ", "300628.SZ", "300628.SZ"],
            "date": [dt.date(2017, 5, 12), dt.date(2017, 9, 29), dt.date(2026, 9, 21)],
            "open": [312.40, 153.84, 36.88],
            "high": [312.40, 153.84, 36.88],
            "low": [312.40, 153.84, 36.88],
            "close": [312.40, 153.84, 36.88],
        }
    )
    out = _apply_adj_factor(raw, events).sort("date")
    closes = out["close"].to_list()

    # 2017-05-12: 312.40 * 0.047329 / 1.0 ≈ 14.786
    assert abs(closes[0] - 14.7856) < 0.01
    # 最新一日锚定为 1.0，价格不变
    assert abs(closes[-1] - 36.88) < 1e-9
    # 拆分段连续：2017-09-28 → 09-29 不应出现腰斩
    assert closes[1] / closes[0] < 1.1


def test_apply_adj_factor_is_noop_on_empty_factors():
    """因子为空时必须原样返回（旧实现正是静默返回 raw，才让问题藏了这么久）。"""
    raw = pl.DataFrame(
        {"symbol": ["A.SZ"], "date": [dt.date(2020, 1, 1)], "close": [10.0]}
    )
    out = _apply_adj_factor(raw, pl.DataFrame())
    assert out["close"].to_list() == [10.0]


def test_resolve_factor_source_prefers_baostock(tmp_path: Path):
    """`adj_factor_baostock` 是权威源，优先于旧 `adj_factor`。"""
    (tmp_path / "adj_factor").mkdir()
    (tmp_path / "adj_factor_baostock").mkdir()
    (tmp_path / "adj_factor" / "all.parquet").write_bytes(b"legacy")
    (tmp_path / "adj_factor_baostock" / "all.parquet").write_bytes(b"auth")

    path, cumulative = _resolve_factor_source(tmp_path)
    assert path.name == "all.parquet"
    assert path.parent.name == "adj_factor_baostock"
    assert cumulative is True


def test_resolve_factor_source_falls_back_to_legacy(tmp_path: Path):
    """只有旧表时回退，且按「逐次事件」解释（cumulative=False）。"""
    (tmp_path / "adj_factor").mkdir()
    (tmp_path / "adj_factor" / "all.parquet").write_bytes(b"legacy")

    path, cumulative = _resolve_factor_source(tmp_path)
    assert path.parent.name == "adj_factor"
    assert cumulative is False


def test_resolve_factor_source_missing_both(tmp_path: Path):
    """两个来源都没有时返回旧路径且 cumulative=False（调用方据此得到空表）。"""
    path, cumulative = _resolve_factor_source(tmp_path)
    assert path.parent.name == "adj_factor"
    assert cumulative is False
    assert _load_factors(path, cumulative=cumulative).is_empty()


def test_load_factors_converts_only_when_cumulative():
    """cumulative=True 才做转换（值变成逐次比值），否则原样返回。"""
    cum = _cum_frame()
    as_is = _load_factors(_write_tmp(cum), cumulative=False)
    assert as_is.height == cum.height
    assert as_is["ex_factor"].to_list() == cum["ex_factor"].to_list()

    converted = _load_factors(_write_tmp(cum), cumulative=True)
    # 转换后不再是累计因子：每行都是 |m-1| > 1e-4 的逐次比值
    ratios = converted["ex_factor"].to_list()
    assert all(abs(r - 1.0) > 1e-4 for r in ratios)
    # 首行 m = f_0 / 1.0
    assert abs(ratios[0] - 0.047066) < 1e-9


def _write_tmp(df: pl.DataFrame) -> Path:
    import tempfile

    fd, name = tempfile.mkstemp(suffix=".parquet")
    import os

    os.close(fd)
    p = Path(name)
    df.write_parquet(p)
    return p
