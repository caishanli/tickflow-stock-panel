"""L0 结构检查：分区逐 symbol 的 bar 数 / 时间戳 / 数值合法性。

停牌、新股首日、提前收盘这些正常场景不能刷屏，所以 bar 数不足只记
``soft``，且停牌名单内的 symbol 直接跳过 ``MISSING_BARS`` / ``ALL_ZERO``。
"""

from datetime import date, datetime

import polars as pl

from app.quant.data_verify.checks_structure import check_structure
from app.quant.data_verify.findings import Finding

DAY = date(2026, 11, 6)


def _frame(rows):
    return pl.DataFrame(rows, schema={
        "symbol": pl.String, "datetime": pl.Datetime("us"),
        "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
        "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
    })


def _day_rows(sym, n=241, start_hour=9, start_min=30):
    out, h, m = [], start_hour, start_min
    for _ in range(n):
        out.append((sym, datetime(2026, 11, 6, h, m), 10.0, 10.1, 9.9, 10.0, 100.0, 1000.0))
        m += 1
        if m == 60:
            h, m = h + 1, 0
        if h == 11 and m == 31:
            h, m = 13, 1
    return out


def test_missing_bars_flagged():
    df = _frame(_day_rows("600000.SH", n=200))
    codes = [f.code for f in check_structure(df, table="kline_minute", day=DAY)]
    assert "MISSING_BARS" in codes


def test_suspended_symbol_with_zero_volume_not_flagged():
    df = _frame([(r[0], r[1], 10.0, 10.0, 10.0, 10.0, 0.0, 0.0) for r in _day_rows("600000.SH")])
    codes = [f.code for f in check_structure(
        df, table="kline_minute", day=DAY, suspended_symbols={"600000.SH"})]
    assert codes == []


def test_suspended_symbol_fewer_bars_not_flagged():
    df = _frame([
        (r[0], r[1], 10.0, 10.0, 10.0, 10.0, 0.0, 0.0)
        for r in _day_rows("600000.SH", n=200)
    ])
    codes = [f.code for f in check_structure(
        df, table="kline_minute", day=DAY, suspended_symbols={"600000.SH"})]
    assert codes == []


def test_duplicate_and_1300_bar_flagged():
    base = _day_rows("600000.SH")
    rows = [*base, base[0], ("600000.SH", datetime(2026, 11, 6, 13, 0), 1, 1, 1, 1, 1, 1)]
    codes = {f.code for f in check_structure(_frame(rows), table="kline_minute", day=DAY)}
    assert "DUPLICATE_BAR" in codes and "FORBIDDEN_1300" in codes


def test_ohlc_invalid_and_non_finite():
    rows = _day_rows("600000.SH")
    rows[0] = ("600000.SH", datetime(2026, 11, 6, 9, 30), 10.0, 9.0, 11.0, 10.0, 100.0, 1000.0)
    rows[1] = ("600000.SH", datetime(2026, 11, 6, 9, 31), float("nan"), 10, 9, 10, 100, 1000)
    codes = {f.code for f in check_structure(_frame(rows), table="kline_minute", day=DAY)}
    assert "OHLC_INVALID" in codes and "NON_FINITE" in codes


def test_ohlc_high_below_close_only_flagged():
    rows = _day_rows("600000.SH")
    # high(10.5) >= min(open, close)=10，但 < close=11：旧实现会漏报。
    rows[0] = ("600000.SH", datetime(2026, 11, 6, 9, 30), 10.0, 10.5, 9.0, 11.0, 100.0, 1000.0)
    ohlc = [f for f in check_structure(_frame(rows), table="kline_minute", day=DAY)
            if f.code == "OHLC_INVALID"]
    assert len(ohlc) == 1
    assert ohlc[0].detail == {"price_order_rows": 1, "rows": 1}


def test_ohlc_low_above_open_only_flagged():
    rows = _day_rows("600000.SH")
    # low(10.5) <= max(open, close)=11，但 > open=10：旧实现会漏报。
    rows[0] = ("600000.SH", datetime(2026, 11, 6, 9, 30), 10.0, 11.5, 10.5, 11.0, 100.0, 1000.0)
    ohlc = [f for f in check_structure(_frame(rows), table="kline_minute", day=DAY)
            if f.code == "OHLC_INVALID"]
    assert len(ohlc) == 1
    assert ohlc[0].detail == {"price_order_rows": 1, "rows": 1}


def test_out_of_session_bar_flagged():
    rows = [
        *_day_rows("600000.SH"),
        ("600000.SH", datetime(2026, 11, 6, 9, 29), 10.0, 10.0, 10.0, 10.0, 1.0, 1.0),
        ("600000.SH", datetime(2026, 11, 6, 15, 1), 10.0, 10.0, 10.0, 10.0, 1.0, 1.0),
    ]
    findings = [f for f in check_structure(_frame(rows), table="kline_minute", day=DAY)
                if f.code == "OUT_OF_SESSION"]
    assert len(findings) == 1
    assert findings[0].detail == {"bars": 2}
    assert findings[0].severity == "hard"


def test_all_zero_volume_not_suspended_is_suspicious():
    df = _frame([(r[0], r[1], 10.0, 10.0, 10.0, 10.0, 0.0, 0.0) for r in _day_rows("600000.SH")])
    findings = check_structure(df, table="kline_minute", day=DAY)
    assert [f.code for f in findings] == ["ALL_ZERO"]
    assert findings[0].severity == "soft"


def test_missing_bars_detail_and_date():
    df = _frame(_day_rows("600000.SH", n=200))
    finding = next(f for f in check_structure(df, table="kline_minute", day=DAY)
                   if f.code == "MISSING_BARS")
    assert finding.detail == {"expected": 241, "actual": 200}
    assert finding.table == "kline_minute"
    assert finding.symbol == "600000.SH"
    assert finding.day == DAY


def test_finding_to_dict_serializes_date():
    finding = Finding(table="kline_minute", day=DAY, symbol="600000.SH",
                      code="MISSING_BARS", severity="soft", detail={"actual": 200})
    assert finding.to_dict() == {
        "table": "kline_minute", "day": "2026-11-06", "symbol": "600000.SH",
        "code": "MISSING_BARS", "severity": "soft", "detail": {"actual": 200},
    }


def test_each_finding_has_its_own_detail_dict():
    rows = [
        *_day_rows("600000.SH"),
        *_day_rows("600001.SH"),
        ("600000.SH", datetime(2026, 11, 6, 9, 29), 10.0, 10.0, 10.0, 10.0, 1.0, 1.0),
        ("600001.SH", datetime(2026, 11, 6, 15, 1), 10.0, 10.0, 10.0, 10.0, 1.0, 1.0),
    ]
    findings = [f for f in check_structure(_frame(rows), table="kline_minute", day=DAY)
                if f.code == "OUT_OF_SESSION"]
    assert len(findings) == 2
    assert findings[0].detail is not findings[1].detail


def test_empty_frame_is_noop():
    assert check_structure(_frame([]), table="kline_minute", day=DAY) == []
