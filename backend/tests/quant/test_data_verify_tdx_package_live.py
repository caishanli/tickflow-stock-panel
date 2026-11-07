"""通达信盘后包真实抓取验收（仅 ``ASTOCK_LIVE=1`` 时运行）。

验收口径（计划 Task 6 Step 4）：``2026-11-06`` 返回 ≥5000 行，``600519`` 的收盘价
与本地 ``kline_daily`` 一致；顺带核对成交量单位（探针为「股」，本地股票日线为「手」）。
"""

import os
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.quant.data_verify.probes.tdx_package import fetch_daily_package

LIVE = os.environ.get("ASTOCK_LIVE") == "1"

pytestmark = pytest.mark.skipif(not LIVE, reason="设置 ASTOCK_LIVE=1 才跑真实抓取")


def _local_daily(day: date) -> pl.DataFrame:
    from app.services.mootdx_service import DATA_ROOT

    path = Path(DATA_ROOT) / "kline_daily" / f"date={day.isoformat()}" / "part.parquet"
    if not path.exists():
        pytest.skip(f"本地日线分区不存在：{path}")
    return pl.read_parquet(path)


def test_fetch_daily_package_matches_local_daily():
    day = date(2026, 11, 6)
    df = fetch_daily_package(day)

    assert df.height >= 5000
    assert df["symbol"].n_unique() == df.height
    assert df["date"].unique().to_list() == [day]

    remote = df.filter(pl.col("symbol") == "600519.SH")
    assert remote.height == 1
    local = _local_daily(day).filter(pl.col("symbol") == "600519.SH")
    assert local.height == 1

    assert remote["close"][0] == pytest.approx(local["close"][0], abs=1e-6)
    assert remote["volume"][0] == pytest.approx(local["volume"][0] * 100, rel=1e-6)
