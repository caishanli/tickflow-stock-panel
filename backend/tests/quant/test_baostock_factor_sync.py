from datetime import date as _date

from app.quant.baostock_factor_sync import expand_to_daily, to_bs_code


def test_to_bs_code():
    assert to_bs_code("000001.SZ") == "sz.000001"
    assert to_bs_code("600000.SH") == "sh.600000"
    assert to_bs_code("920371.BJ") == "bj.920371"


def test_expand_step_function_value_applies_from_event_day():
    """事件日的因子从当天起生效（除权后口径），并保持到下一事件日之前。"""
    events = [(_date(2016, 6, 16), 0.737370), (_date(2017, 7, 21), 0.748283)]
    daily = expand_to_daily("000001.SZ", events, _date(2016, 1, 1), _date(2017, 12, 31))
    m = dict(daily)
    assert m[_date(2016, 6, 15)] == 0.737370      # 事件前：沿用前一事件的值
    assert m[_date(2016, 6, 16)] == 0.737370      # 事件日当天：切换为新值
    assert m[_date(2017, 7, 20)] == 0.737370
    assert m[_date(2017, 7, 21)] == 0.748283      # 下一事件日：再切换
    assert m[_date(2017, 12, 31)] == 0.748283


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
