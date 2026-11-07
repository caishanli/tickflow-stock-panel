"""``checks_factors``：本地 qfq（raw × ex_factor）vs 外部 qfq 的台阶/偏移判定。

对齐 2026-10-22 事故形态：回补数据被写成**已前复权价**，读侧
``DataSources.apply_qfq_daily`` 再乘一次 ``ex_factor`` → 比例序列出现台阶
（部分区间重复复权）；整段因子口径错位则表现为恒定比例偏移。
"""

from datetime import date

import polars as pl

from app.quant.data_verify.checks_factors import apply_local_qfq, compare_qfq

DATES = [date(2023, 4, 19), date(2023, 6, 1), date(2023, 12, 20)]


def _raw():
    return pl.DataFrame(
        [("600000.SH", d, 10.0) for d in DATES],
        schema={"symbol": pl.String, "date": pl.Date, "close": pl.Float64}, orient="row")


def _factors(factor):
    return pl.DataFrame(
        [("600000.SH", d, factor) for d in DATES],
        schema={"symbol": pl.String, "date": pl.Date, "ex_factor": pl.Float64}, orient="row")


def _external(close_base=7.5):
    return pl.DataFrame(
        [("600000.SH", d, close_base) for d in DATES],
        schema={"symbol": pl.String, "date": pl.Date, "qfq_close": pl.Float64}, orient="row")


def test_double_applied_factor_produces_step():
    local = apply_local_qfq(_raw(), _factors(0.5625))    # 0.75 被用了两次
    findings = compare_qfq(local, _external(7.5), symbol="600000.SH")
    assert [f.code for f in findings] == ["QFQ_OFFSET"]
    local2 = apply_local_qfq(_raw(), _factors(0.5625)).with_columns(
        pl.when(pl.col("date") >= date(2023, 6, 1))
        .then(pl.col("qfq_close") * 0.75).otherwise(pl.col("qfq_close")).alias("qfq_close"))
    findings2 = compare_qfq(local2, _external(7.5), symbol="600000.SH")
    assert "QFQ_STEP" in [f.code for f in findings2]


def test_consistent_factor_has_no_finding():
    local = apply_local_qfq(_raw(), _factors(1.0))
    assert compare_qfq(local, _external(10.0), symbol="600000.SH") == []


# --- 以下为 brief 之外的补强用例（空输入 / 容差内偏移 / 因子缺口回填）-----------


def test_empty_inputs_have_no_finding():
    empty_local = _raw().head(0).with_columns(pl.lit(None, dtype=pl.Float64).alias("qfq_close"))
    empty_external = _external().head(0)
    assert compare_qfq(empty_local, empty_external, symbol="600000.SH") == []


def test_offset_within_tolerance_is_ignored():
    # 0.1% 偏移 < 默认 rel_tol 0.5%，且比例恒定 → 无台阶、无偏移。
    local = apply_local_qfq(_raw(), _factors(1.0)).with_columns(
        (pl.col("qfq_close") * 0.999).alias("qfq_close"))
    assert compare_qfq(local, _external(10.0), symbol="600000.SH") == []


def test_missing_factor_day_backfills_first_factor():
    # 因子表缺首日行 → 回填该标的首个因子（与 apply_qfq_daily 同口径），不按 1.0 处理。
    factors = _factors(0.8).filter(pl.col("date") > DATES[0])
    local = apply_local_qfq(_raw(), factors)
    closes = local.sort("date")["qfq_close"].to_list()
    assert closes == [8.0, 8.0, 8.0]


def test_unknown_symbol_has_no_finding():
    local = apply_local_qfq(_raw(), _factors(1.0))
    assert compare_qfq(local, _external(10.0), symbol="000001.SZ") == []
