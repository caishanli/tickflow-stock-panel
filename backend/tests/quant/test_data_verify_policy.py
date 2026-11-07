"""日线 L2 外部比对 + 仲裁决策。

L2 把本地 ``kline_daily`` 分区与外部日线源（通达信盘后包 / baostock / 腾讯）
逐项比对产出 :class:`Finding`；:func:`decide` 再按 design §5.4 的多数规则把
结论翻译成 :class:`Decision`（修 / 保留 / 隔离）。

判定口径（与 docs/superpowers/specs/2026-11-07-multisource-data-verification-design.md
§5.2 一致）：

- 价格容差 ``max(绝对 0.011, 相对 0.1% × 本地价)`` → ``DAILY_CLOSE_MISMATCH``（hard）；
- 成交量相对容差 0.5% → ``DAILY_VOLUME_MISMATCH``（hard）；
- 成交额相对容差 0.5%，先按双方 ``close × volume`` 归一（否则价格/量本身的
  差异会连带把 amount 判错）→ ``DAILY_AMOUNT_MISMATCH``（soft，只报告）。
"""

from datetime import date

import polars as pl
import pytest

from app.quant.data_verify.checks_external import compare_daily_external
from app.quant.data_verify.policy import decide

DAY = date(2026, 11, 6)


def _df(close, volume=1000.0):
    return pl.DataFrame([("600000.SH", DAY, 10.0, 10.2, 9.8, close, volume, close * volume)],
                        schema={"symbol": pl.String, "date": pl.Date, "open": pl.Float64,
                                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                                "volume": pl.Float64, "amount": pl.Float64})


def _row(symbol="600000.SH", close=10.0, volume=1000.0, amount=None, day=DAY):
    """单行日线帧（``amount`` 缺省 = ``close × volume``）。"""
    return (symbol, day, close, close, close, close, volume,
            close * volume if amount is None else amount)


def _frame(*rows):
    return pl.DataFrame(rows, orient="row",
                        schema={"symbol": pl.String, "date": pl.Date, "open": pl.Float64,
                                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                                "volume": pl.Float64, "amount": pl.Float64})


# --- brief 的四个用例（原样保留） ---------------------------------------------


def test_close_mismatch_detected():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    assert [x.code for x in f] == ["DAILY_CLOSE_MISMATCH"]


def test_two_external_agree_external_repairs():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"tdx": "agree_external", "baostock": "agree_external"})
    assert d[0].action == "repair_local"


def test_single_source_marks_single_source():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"tdx": "agree_external"})
    assert d[0].action == "repair_local" and d[0].finding.detail["single_source"] is True


def test_conflict_quarantines():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"tdx": "agree_external", "baostock": "agree_local"})
    assert d[0].action == "quarantine"


# --- 比对口径 ----------------------------------------------------------------


def test_volume_mismatch_is_hard():
    f = compare_daily_external(_df(10.0), _df(10.0, volume=2000.0), source="tdx")
    assert [x.code for x in f] == ["DAILY_VOLUME_MISMATCH"]
    assert f[0].severity == "hard"
    assert f[0].table == "kline_daily" and f[0].day == DAY and f[0].symbol == "600000.SH"


def test_volume_within_half_percent_no_finding():
    # 0.4% 的量差属跨源口径差异，不该报。
    f = compare_daily_external(_df(10.0), _df(10.0, volume=1004.0), source="tdx")
    assert f == []


def test_amount_mismatch_is_soft():
    # 价格、量都一致，仅成交额差 3% → soft（成交额只报告，不驱动修复）。
    local = _frame(_row(amount=10_000.0))
    external = _frame(_row(amount=10_300.0))
    f = compare_daily_external(local, external, source="tdx")
    assert [x.code for x in f] == ["DAILY_AMOUNT_MISMATCH"]
    assert f[0].severity == "soft"


def test_amount_not_falsely_flagged_when_close_differs():
    # brief 的 close 用例里 amount = close × volume，价格差异会连带 amount 差异；
    # 按 close×volume 归一后只剩一条 close 结论（否则每条价格差都会附带一条噪音）。
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    assert "DAILY_AMOUNT_MISMATCH" not in [x.code for x in f]


def test_price_tolerance_is_max_of_abs_and_rel():
    # 10.0 vs 10.005：差 0.005 < 绝对容差 0.011。
    assert compare_daily_external(_df(10.0), _df(10.005), source="tdx") == []
    # 1000.0 vs 1000.5：差 0.5 > 绝对容差，但 < 相对容差 1.0。
    assert compare_daily_external(_df(1000.0), _df(1000.5), source="tdx") == []
    # 1000.0 vs 1002.0：差 2.0 > 相对容差 1.0 → 报警。
    codes = [x.code for x in compare_daily_external(_df(1000.0), _df(1002.0), source="tdx")]
    assert codes == ["DAILY_CLOSE_MISMATCH"]


def test_missing_symbols_ignored():
    # 只比交集：外部包含指数/ETF、本地缺 symbol 都不该产出结论（覆盖度是 Task 11 的事）。
    external = _frame(_row(), _row(symbol="999999.SH", close=3000.0),
                      _row(symbol="510300.SH", close=4.0))
    local = _frame(_row(), _row(symbol="000001.SZ"))
    assert compare_daily_external(local, external, source="tdx") == []


def test_volume_factor_bridges_shares_and_lots():
    # 通达信包的 volume 是「股」，kline_daily 存「手」：调用方必须显式传 0.01，
    # 否则全市场都会被判 volume 错（100 倍）。amount 单位两侧都是元，故显式给。
    local = _frame(_row(volume=1000.0, amount=1_000_000.0))
    external = _frame(_row(volume=100_000.0, amount=1_000_000.0))
    assert compare_daily_external(local, external, source="tdx", volume_factor=0.01) == []
    codes = [x.code for x in compare_daily_external(local, external, source="tdx")]
    assert "DAILY_VOLUME_MISMATCH" in codes


def test_source_is_recorded_in_detail():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    assert f[0].detail["source"] == "tdx"


def test_empty_inputs_yield_no_findings():
    empty = pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date, "close": pl.Float64,
                                 "volume": pl.Float64, "amount": pl.Float64})
    assert compare_daily_external(empty, _df(10.0), source="tdx") == []
    assert compare_daily_external(_df(10.0), empty, source="tdx") == []


def test_missing_close_column_raises():
    bare = pl.DataFrame([("600000.SH", DAY, 10.0)],
                        schema={"symbol": pl.String, "date": pl.Date, "volume": pl.Float64},
                        orient="row")
    with pytest.raises(ValueError):
        compare_daily_external(bare, _df(10.0), source="tdx")


# --- 仲裁规则 ----------------------------------------------------------------


def test_confirmed_marks_confirmed_and_prefers_priority_source():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"baostock": "agree_external", "tdx": "agree_external"})
    assert d[0].finding.detail["confirmed"] is True
    assert "single_source" not in d[0].finding.detail
    assert d[0].source == "tdx"          # §5.4 优先级：通达信 > baostock


def test_soft_finding_never_repairs():
    local = _frame(_row(amount=10_000.0))
    external = _frame(_row(amount=10_300.0))
    f = compare_daily_external(local, external, source="tdx")
    d = decide(f, {"tdx": "agree_external", "baostock": "agree_external"})
    assert d[0].action == "keep_local"


def test_agree_local_keeps_local():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"tdx": "agree_external", "baostock": "agree_local",
                   "tencent": "agree_local"})
    assert d[0].action == "keep_local"


def test_unknown_verdict_keeps_local():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    assert [x.action for x in decide(f, {"tdx": "unknown"})] == ["keep_local"]
    assert [x.action for x in decide(f, {})] == ["keep_local"]


def test_invalid_verdict_raises():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    with pytest.raises(ValueError):
        decide(f, {"tdx": "agree-external"})


def test_decide_one_decision_per_finding():
    f = compare_daily_external(_df(10.0), _df(10.5, volume=5000.0), source="tdx")
    assert [x.code for x in f] == ["DAILY_CLOSE_MISMATCH", "DAILY_VOLUME_MISMATCH"]
    d = decide(f, {"tdx": "agree_external", "baostock": "agree_external"})
    assert [x.finding.code for x in d] == [x.code for x in f]
    assert {x.action for x in d} == {"repair_local"}
