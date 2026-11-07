"""多源数据校验的单位契约。

腾讯 mkline 分钟的成交量单位随板块而变（主板/创业板/ETF 给「手」，科创板
688/689 直接给「股」），日线落盘单位也不统一（``kline_daily`` 手、
``kline_etf_daily`` 股）。跨源比对之前必须先把单位契约钉死，否则 100 倍
偏差会被当成"数据源不一致"。
"""

import pytest

from app.quant.data_verify.units import (
    DAILY_VOLUME_UNIT,
    minute_volume_to_daily,
    tencent_m1_volume_factor,
)


@pytest.mark.parametrize("symbol,expected", [
    ("688001.SH", 1.0), ("689009.SH", 1.0),   # 科创板：腾讯 m1 原始量已是股
    ("600000.SH", 100.0), ("000001.SZ", 100.0),
    ("300750.SZ", 100.0), ("510300.SH", 100.0), ("588000.SH", 100.0),
])
def test_tencent_m1_volume_factor(symbol, expected):
    assert tencent_m1_volume_factor(symbol) == expected


def test_minute_volume_to_daily():
    assert minute_volume_to_daily(8_630_559.0, "kline_minute") == pytest.approx(86_305.59)
    assert minute_volume_to_daily(9_802_800_000.0, "kline_etf_minute") == 9_802_800_000.0


def test_daily_volume_unit_contract():
    assert DAILY_VOLUME_UNIT["kline_daily"] == "lot"
    assert DAILY_VOLUME_UNIT["kline_etf_daily"] == "share"
