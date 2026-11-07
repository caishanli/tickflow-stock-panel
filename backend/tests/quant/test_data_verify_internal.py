"""L1 分钟→日线内部聚合比对。

这一层不需要任何网络源：把当日分钟按 symbol 聚合成「日线形态」，与同日
``kline_daily`` / ``kline_etf_daily`` 对拍。2760 个分钟分区（2016 起）全靠它
覆盖，科创板 ×100 这类量级污染也是在这里被抓出来的。
"""

from datetime import date, datetime

import polars as pl

from app.quant.data_verify.checks_internal import aggregate_minute, compare_minute_to_daily

DAY = date(2026, 11, 6)


def _minute(sym, volumes):
    return pl.DataFrame(
        [(sym, datetime(2026, 11, 6, 9, 30 + i), 10.0, 10.1, 9.9, 10.0, v, v * 10.0)
         for i, v in enumerate(volumes)],
        schema={"symbol": pl.String, "datetime": pl.Datetime("us"),
                "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
                "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64},
    )


def _daily(sym, volume, amount, close=10.0):
    return pl.DataFrame([(sym, DAY, 10.0, 10.1, 9.9, close, volume, amount)],
                        schema={"symbol": pl.String, "date": pl.Date, "open": pl.Float64,
                                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                                "volume": pl.Float64, "amount": pl.Float64})


def test_close_uses_last_nonzero_bar():
    m = _minute("600004.SH", [100.0, 200.0, 0.0, 0.0])
    m = m.with_columns(pl.when(pl.int_range(pl.len()) >= 2).then(pl.lit(13.27)).otherwise(pl.col("close")).alias("close"))
    agg = aggregate_minute(m)
    assert agg["m_close"][0] == 10.0      # 尾段 0 量平推不参与收盘价


def test_star_volume_x100_detected():
    # 688001 当日真实量 86305.59 手 = 8,630,559 股；被旧解析器（原值已是股又 ×100）
    # 写成 863,055,900「股」——单位归一后比值 ≈100，这才是污染指纹。
    m = _minute("688001.SH", [863_055_900.0])        # 股（被 ×100）
    d = _daily("688001.SH", volume=86_305.59, amount=8.63e8)
    codes = {f.code for f in compare_minute_to_daily(m, d, table="kline_minute")}
    assert "MINUTE_VOLUME_X100" in codes


def test_star_correct_volume_not_flagged():
    # 修复后的同一标的（8,630,559 股 vs 86,305.59 手）必须干净——否则 runner 的
    # 「已修复 → 重跑 repair=0」幂等回归（Task 5）会被反复 ×100。
    m = _minute("688001.SH", [8_630_559.0])          # 股
    d = _daily("688001.SH", volume=86_305.59, amount=8.63e8)
    findings = compare_minute_to_daily(m, d, table="kline_minute")
    volume_codes = {
        f.code for f in findings
        if f.code in ("MINUTE_VOLUME_X100", "MINUTE_VOLUME_MISMATCH")
    }
    assert volume_codes == set()


def test_etf_volume_compared_share_to_share():
    m = _minute("510300.SH", [9_802_800.0])
    d = _daily("510300.SH", volume=9_802_800.0, amount=9.8028e9)
    codes = {f.code for f in compare_minute_to_daily(m, d, table="kline_etf_minute")}
    assert "MINUTE_VOLUME_MISMATCH" not in codes


def test_amount_mismatch_is_soft():
    m = _minute("600000.SH", [1_000_000.0])          # amount = 1e7
    d = _daily("600000.SH", volume=10_000.0, amount=1.08e7)   # 8% 偏差
    findings = compare_minute_to_daily(m, d, table="kline_minute")
    amt = [f for f in findings if f.code == "MINUTE_AMOUNT_MISMATCH"]
    assert amt and amt[0].severity == "soft"


def test_suspended_day_zero_daily_volume_no_finding():
    m = _minute("600000.SH", [0.0, 0.0])
    d = _daily("600000.SH", volume=0.0, amount=0.0)
    assert compare_minute_to_daily(m, d, table="kline_minute") == []


def test_minute_close_mismatch_is_soft():
    # 2016-01-04 600004：分钟最后一根有量 bar 收 13.27，日线收 13.04（提前收盘）
    m = _minute("600004.SH", [100.0, 0.0])
    d = _daily("600004.SH", volume=1.0, amount=10.0, close=13.04)
    findings = compare_minute_to_daily(m, d, table="kline_minute")
    close_f = [f for f in findings if f.code == "MINUTE_CLOSE_MISMATCH"]
    assert close_f and close_f[0].severity == "soft"
