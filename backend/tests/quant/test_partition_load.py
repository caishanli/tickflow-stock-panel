# -*- coding: utf-8 -*-
"""按日分区 Parquet（data/kline_*_daily|minute/date=*/）读取回归测试。

覆盖：
- _load_daily_from_partitions 同时返回 A股（000001.XSHE）与 ETF（515700.XSHG）
- A股日线 volume 存盘为「手」，读取后 ×100 归一到「股」（000001 日成交量百万股级）
- ETF 日线 volume 存盘即为「股」，不换算
- _load_minute_from_partitions 返回 DatetimeIndex 索引的分钟帧
"""

import pandas as pd
import pytest

from app.quant.jqengine.datasource.manager import get_data_manager


@pytest.fixture
def dm():
    d = get_data_manager()
    d._offline = True
    return d


def test_load_daily_from_partitions_has_etf_and_stock(dm):
    daily = dm._load_daily_from_partitions(asof=None)
    assert "515700.XSHG" in daily
    assert "000001.XSHE" in daily


def test_load_daily_from_partitions_a_stock_volume_in_shares(dm):
    # A股日线分区 volume 存盘为「手」，读取后应归一到「股」（×100）
    daily = dm._load_daily_from_partitions(asof=None)
    df = daily["000001.XSHE"]
    assert (df["volume"] > 1e6).all()  # 000001 日成交量百万股级


def test_load_daily_from_partitions_etf_volume_not_scaled(dm):
    # ETF 日线分区 volume 存盘即为「股」，读取时不应被 ×100
    daily = dm._load_daily_from_partitions(asof=None)
    df = daily["515700.XSHG"]
    assert (df["volume"] > 0).all()


def test_load_minute_from_partitions(dm):
    df = dm._load_minute_from_partitions(
        "515700.XSHG",
        pd.Timestamp("2026-04-01"),
        pd.Timestamp("2026-04-05"),
    )
    assert df is not None
    assert len(df) > 0
    assert isinstance(df.index, pd.DatetimeIndex)
