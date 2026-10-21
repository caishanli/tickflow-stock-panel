"""``baostock_factor_sync`` 的回归测试（回源 → 因子表）。

覆盖三条契约：
1. step function 语义：事件日的因子从当天起生效；
2. **首个事件之前**的取值 = ``fore(首事件) × m(首事件)``（累计因子在事件日跳变，
   事件之前更小）——直接沿用 fore(首事件) 会让首个事件在 step function 上
   「不跳变」，``daily_to_events`` 会把该事件整条丢掉；
3. 事件行表直构（``build_event_rows``）与「逐日表 → daily_to_events」等价。
"""

from datetime import date as _date

import polars as pl

from app.quant.baostock_factor_sync import (
    build_event_rows,
    daily_to_events,
    expand_to_daily,
    to_bs_code,
)


def test_to_bs_code():
    assert to_bs_code("000001.SZ") == "sz.000001"
    assert to_bs_code("600000.SH") == "sh.600000"
    assert to_bs_code("920371.BJ") == "bj.920371"


def test_expand_step_function_value_applies_from_event_day():
    """事件日的因子从当天起生效（除权后口径），并保持到下一事件日之前。"""
    events = [(_date(2016, 6, 16), 0.737370), (_date(2017, 7, 21), 0.748283)]
    daily = expand_to_daily("000001.SZ", events, _date(2016, 1, 1), _date(2017, 12, 31))
    m = dict(daily)
    # 事件前：累计因子比事件日更小（乘上该事件的乘数 m1 = f1/f2）
    m1 = 0.737370 / 0.748283
    assert m[_date(2016, 6, 15)] == 0.737370 * m1
    assert m[_date(2016, 6, 16)] == 0.737370      # 事件日当天：切换为新值
    assert m[_date(2017, 7, 20)] == 0.737370
    assert m[_date(2017, 7, 21)] == 0.748283      # 下一事件日：再切换
    assert m[_date(2017, 12, 31)] == 0.748283


def test_expand_pre_first_event_value_is_smaller_than_event_value():
    """首个事件之前的值必须**小于**事件日当天的值（否则首个事件不跳变）。"""
    events = [(_date(2016, 6, 16), 0.737370), (_date(2017, 7, 21), 0.748283)]
    daily = dict(expand_to_daily("000001.SZ", events, _date(2016, 1, 1), _date(2017, 12, 31)))
    before = daily[_date(2016, 1, 1)]
    at_event = daily[_date(2016, 6, 16)]
    assert before < at_event, "首个事件前后的值相同 → 该事件会被 daily_to_events 丢掉"


def test_expand_uses_latest_event_value_after_last_event():
    """最后事件日之后一直用最后一个因子（=1.0，最新锚定）。"""
    events = [(_date(2025, 10, 15), 0.947276), (_date(2026, 9, 24), 1.0)]
    daily = expand_to_daily("000001.SZ", events, _date(2026, 4, 1), _date(2026, 9, 30))
    m = dict(daily)
    assert m[_date(2026, 4, 1)] == 0.947276
    assert m[_date(2026, 9, 24)] == 1.0
    assert m[_date(2026, 9, 30)] == 1.0


def test_expand_respects_start_boundary():
    """start 之前的日期不产出。"""
    events = [(_date(2016, 6, 16), 0.737370)]
    daily = expand_to_daily("000001.SZ", events, _date(2020, 1, 1), _date(2020, 1, 10))
    assert all(d >= _date(2020, 1, 1) for d, _ in daily)
    assert len(daily) == 10
    assert daily[0][1] == 0.737370


def test_expand_empty_events():
    assert expand_to_daily("X.SZ", [], _date(2020, 1, 1), _date(2020, 1, 10)) == []


def _events_to_daily_frame(events, start, end):
    rows = expand_to_daily("X.SZ", events, start, end)
    return pl.DataFrame(
        {"symbol": ["X.SZ"] * len(rows),
         "trade_date": [d for d, _ in rows],
         "ex_factor": [f for _, f in rows]},
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})


def test_build_event_rows_matches_daily_to_events():
    """事件行表直构 ≡ 逐日表 → daily_to_events（内存友好路径的等价性）。"""
    events = [(_date(2016, 1, 1), 0.605293), (_date(2016, 6, 16), 0.737370),
              (_date(2017, 7, 21), 0.748283), (_date(2018, 7, 12), 0.760408)]
    start = _date(2016, 1, 1)
    end = _date(2026, 12, 31)
    daily = _events_to_daily_frame(events, start, end)
    via_daily = daily_to_events(daily, start)
    direct = pl.DataFrame(
        {"symbol": ["X.SZ"] * len(build_event_rows("X.SZ", events, start)),
         "trade_date": [d for d, _ in build_event_rows("X.SZ", events, start)],
         "ex_factor": [f for _, f in build_event_rows("X.SZ", events, start)]},
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    assert direct.sort(["symbol", "trade_date"]).rows() == \
        via_daily.sort(["symbol", "trade_date"]).rows()


def test_build_event_rows_keeps_first_real_event_when_baseline_missing():
    """没有基线行时，首个真实事件必须出现在事件行表里（不能被丢掉）。"""
    events = [(_date(2020, 6, 10), 0.9), (_date(2023, 7, 20), 1.0)]
    rows = build_event_rows("X.SZ", events, _date(2016, 1, 1))
    dates = [d for d, _ in rows]
    assert dates[0] == _date(2016, 1, 1)
    assert _date(2020, 6, 10) in dates, "首个真实事件被丢掉"
    assert _date(2023, 7, 20) in dates


def test_publish_refuses_lower_coverage(tmp_path):
    """覆盖度不足时拒绝发布（``--limit 50`` 曾把 5531 只的表换成 50 只）。"""
    from app.quant.baostock_factor_sync import _publish
    auth = tmp_path / "all.parquet"
    pl.DataFrame({"symbol": [f"S{i}.SZ" for i in range(100)],
                  "trade_date": [_date(2020, 1, 1)] * 100,
                  "ex_factor": [1.0] * 100}).write_parquet(auth)
    staging = tmp_path / "staging.parquet"
    pl.DataFrame({"symbol": ["S0.SZ"], "trade_date": [_date(2020, 1, 1)],
                  "ex_factor": [1.0]}).write_parquet(staging)
    ok, msg = _publish(staging, auth, new_symbols=1, old_symbols=100, force=False)
    assert ok is False and "拒绝覆盖" in msg
    assert auth.exists(), "被拒绝时权威表必须保持原样"
    # 强制发布 → 生效且留备份
    ok, _msg = _publish(staging, auth, new_symbols=1, old_symbols=100, force=True)
    assert ok is True
    assert auth.with_name(auth.name + ".bak").exists()
