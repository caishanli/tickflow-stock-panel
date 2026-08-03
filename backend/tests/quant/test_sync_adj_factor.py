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
