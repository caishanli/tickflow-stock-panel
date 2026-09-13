"""新浪交易日历解码器 + 校验单测（零网络，payload 为 2026-09-13 实测原文内联）。"""

from datetime import date

import pytest

from app.services import trade_calendar as tc

SINA_PAYLOAD_20260913 = 'LC/AAApNDXCw6mHbaPgkryxXv10eAJP1LW0SD39aT7+NV44Xba3PxCgTdrp5BkYVAc11hWvg0c/19UAc7jNtHQyWBAu2xmGuZI1NVAc3FepphjnTBw1X4hmGu+ypVAcvFenpBXPqCc6F4ZmGueLFwbIN8QTDXPsCc1FepphjvOoCc8FepphjvcgFO3CP00wxXXWhrkUdZrIJpw9X3ThrlEp6hlGc88Kcem0VeFpZM46VV4MrTC2KScKc811U4aLXUdlzINc9lTrwFW3T52KPj0mDueVFuUR1RtiEoCXfdgFOOSGRXnUhrXWhb0kt6Rk2pU44JV4SrTyU9wSDHPwCnXdP1FuiUM44r7qwdKqcYrIZpw1DqgrlU5IrHRawxjrwBaqcbrIt9gr3UhDtOpyVNjEnCHPnC3royNWvi0gjHXBXYdRlLbFpdJFueSFcqkK30sSDO+68K46IVOwVkaBX/'


def test_decode_real_payload():
    dates = tc.decode_sina_calendar(SINA_PAYLOAD_20260913)
    assert len(dates) == 8797  # 新浪原始 8796 + 补 1992-05-04
    assert dates[0] == "1990-12-19"
    assert dates[-1] == "2026-12-31"
    assert "1992-05-04" in dates
    assert "2026-09-10" in dates and "2026-09-11" in dates
    assert "2026-09-12" not in dates and "2026-10-01" not in dates


def test_decode_bad_payload_raises():
    with pytest.raises(ValueError):
        tc.decode_sina_calendar("!!!not-base64!!!___")


def test_validate_rejects():
    with pytest.raises(ValueError):
        tc.validate_calendar(["2026-09-11", "2026-09-10"])  # 非单调
    with pytest.raises(ValueError):
        tc.validate_calendar(["2020-01-02"], today=date(2026, 9, 13))  # 太短且过期
