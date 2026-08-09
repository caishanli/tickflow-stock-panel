"""ETF 单位净值回源：akshare 全市场净值 → 按日分区落盘。

净值收盘后（基金公司披露）才有当日值，盘中只有昨日净值。策略用
``context.previous_date`` 取昨日，与分区日期对齐。幂等：某日分区已存在
则跳过。触发点：启动 backfill、15:35 cron、00:00 巡检。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
from datetime import date as _date
from pathlib import Path

import polars as pl

logger = logging.getLogger("app.services.etf_nav_service")

_env_root = os.getenv("PARTITION_DATA_ROOT", "").strip()
if _env_root:
    DATA_ROOT = Path(_env_root)
else:
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))
    DATA_ROOT = Path(_repo_root) / "data"

ETF_NAV_ROOT = DATA_ROOT / "etf_nav"


def _jq_symbol(code: str) -> str:
    """6 位场内代码 → JQ 代码（5/6/9 沪市 XSHG，其余深市 XSHE）。"""
    code = str(code).strip()
    if code.startswith(("5", "6", "9")):
        return f"{code}.XSHG"
    return f"{code}.XSHE"


def _fund_etf_fund_daily_em() -> pl.DataFrame:
    """akshare 全市场场内 ETF 净值（基金代码/单位净值）。"""
    import akshare as ak
    df = ak.fund_etf_fund_daily_em()
    return pl.from_pandas(df[["基金代码", "单位净值"]])


def sync_etf_nav(day: _date | None = None) -> int:
    """同步指定日（默认今天）全市场 ETF 单位净值到按日分区。幂等。

    返回写入行数；该日分区已存在返回 0。
    """
    day = day or _date.today()
    pdir = ETF_NAV_ROOT / f"date={day}"
    part = pdir / "part.parquet"
    if part.exists():
        logger.info("etf_nav_service: %s 已存在，跳过", day)
        return 0
    raw = _fund_etf_fund_daily_em()
    if raw.is_empty():
        from app.services.mootdx_service import _append_failure
        _append_failure(f"etf_nav:{day}", "empty_akshare")
        return 0
    df = raw.with_columns(
        pl.col("基金代码").map_elements(_jq_symbol, return_dtype=pl.Utf8).alias("symbol"),
        pl.col("单位净值").cast(pl.Float64).alias("unit_nav"),
        pl.lit(day.isoformat()).alias("date"),
    ).select(["symbol", "unit_nav", "date"])
    pdir.mkdir(parents=True, exist_ok=True)
    tmp = pdir / "part.tmp"
    df.sort("symbol").write_parquet(tmp)
    tmp.rename(part)
    logger.info("etf_nav_service: 净值落盘 %d 行 → %s", df.height, part)
    return df.height


def _partition_dates() -> list[str]:
    if not ETF_NAV_ROOT.is_dir():
        return []
    return sorted(d.name[5:] for d in ETF_NAV_ROOT.iterdir()
                  if d.is_dir() and d.name.startswith("date="))


def _market_closed(now: _dt.datetime | None = None) -> bool:
    """当前是否已收盘（≥15:00）。对齐 mootdx_service 口径。"""
    from app.services.mootdx_service import _market_closed as _mc
    return _mc(now)


def _missing_etf_nav_days(now: _dt.datetime | None = None) -> list[_date]:
    """找出分区缺失的净值交易日（最新分区日期 → 今天）。

    盘中（<15:00）不把今天当缺失：当日净值尚未披露。收盘后算缺失。
    """
    from app.services.mootdx_service import _trade_days_up_to
    existing = _partition_dates()
    now = now or _dt.datetime.now()
    today = now.date()
    if not existing:
        if _market_closed(now):
            return _trade_days_up_to(today)
        return []
    latest = _date.fromisoformat(existing[-1])
    if latest >= today and not _market_closed(now):
        return []
    return [d for d in _trade_days_up_to(today) if latest < d <= today]
