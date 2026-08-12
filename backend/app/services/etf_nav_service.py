"""ETF 单位净值回源：akshare 全市场净值 → 按日分区落盘。

净值收盘后（基金公司披露）才有当日值，盘中只有昨日净值。策略用
``context.previous_date`` 取昨日，与分区日期对齐。幂等：某日分区已存在
则跳过。触发点：启动 backfill、15:35 cron、00:00 巡检。
分区日期 = 净值实际披露日（从 akshare 列名解析，形如 ``2026-08-07-单位净值``）。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import re
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


_NAV_DATE_COL_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-单位净值$")


def _fund_etf_fund_daily_em() -> pl.DataFrame:
    """akshare 全市场场内 ETF 净值快照（原始列，含基金代码列）。

    akshare 列名嵌入净值披露日期，形如 ``基金代码 / 2026-08-07-单位净值 /
    2026-08-07-累计净值 / 2026-08-06-单位净值 / ...``。净值披露日期由
    ``_nav_date_from_columns`` 从列名解析。
    """
    import akshare as ak
    df = ak.fund_etf_fund_daily_em()
    return pl.from_pandas(df)


def _nav_date_from_columns(raw: pl.DataFrame) -> _date | None:
    """从 akshare 列名解析最新净值披露日期（``YYYY-MM-DD-单位净值``）。"""
    best: _date | None = None
    for col in raw.columns:
        m = _NAV_DATE_COL_RE.match(str(col))
        if m:
            d = _date.fromisoformat(m.group(1))
            if best is None or d > best:
                best = d
    return best


def sync_etf_nav(day: _date | None = None) -> int:
    """同步全市场 ETF 单位净值到按日分区。幂等。

    分区日期 = 净值实际披露日：从 akshare 列名解析（最新 ``YYYY-MM-DD-单位净值``
    列的日期），``day`` 仅在解析失败时兜底（默认今天）。某日分区已存在返回 0。
    """
    fallback = day or _date.today()
    raw = _fund_etf_fund_daily_em()
    if raw.is_empty():
        from app.services.mootdx_service import _append_failure
        _append_failure(f"etf_nav:{fallback}", "empty_akshare")
        return 0
    nav_date = _nav_date_from_columns(raw)
    if nav_date is None:
        nav_date = fallback
        unit_col = "单位净值"
        logger.warning(
            "etf_nav_service: 无法从 akshare 列名解析净值披露日期，按 %s 落盘", nav_date)
    else:
        unit_col = f"{nav_date.isoformat()}-单位净值"
    if unit_col not in raw.columns:
        from app.services.mootdx_service import _append_failure
        _append_failure(f"etf_nav:{nav_date}", "no_nav_column")
        return 0
    pdir = ETF_NAV_ROOT / f"date={nav_date}"
    part = pdir / "part.parquet"
    if part.exists():
        logger.info("etf_nav_service: %s 已存在，跳过", nav_date)
        return 0
    df = raw.select(pl.col("基金代码"), pl.col(unit_col)).rename(
        {unit_col: "单位净值"})
    df = df.with_columns(
        pl.col("基金代码").map_elements(_jq_symbol, return_dtype=pl.Utf8).alias("symbol"),
        # akshare 对当日无净值/停牌的 ETF 返回 "---"，严格 cast 会拖垮整个分区
        # 写入；容错解析（先转字符串）→ 丢弃解析不出数值的行。
        pl.col("单位净值").cast(pl.Utf8, strict=False)
                          .str.strip_chars()
                          .str.replace("---", "", literal=True)
                          .str.contains(r"^-?\d+(\.\d+)?$", strict=False)
                          .alias("_valid"),
        pl.col("单位净值").cast(pl.Utf8, strict=False)
                          .str.strip_chars()
                          .cast(pl.Float64, strict=False)
                          .alias("unit_nav"),
        pl.lit(nav_date.isoformat()).alias("date"),
    ).filter(pl.col("_valid")).drop("_valid").select(["symbol", "unit_nav", "date"])
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
    """返回需回源的净值交易日：**最多一个**（最新缺失日，历史不补）。

    akshare ``fund_etf_fund_daily_em`` 只有当前快照（无逐日历史），回补历史
    只会把今日净值写成过去每一天（假数据）。因此本函数只返回最新应披露净值的
    交易日中缺失的一个：
    - 交易日当天盘中（<15:00）当日净值未披露，最新已披露净值属前一交易日
      → 候选 = 前一交易日（00:00 巡检即落此分支）；
    - 收盘后（≥15:00）当日净值已披露 → 候选 = 最新交易日；
    - 非交易日（周末/节假日）→ 候选 = 最近一个交易日；
    - 已有分区日期 >= 候选 → 无需回源。
    """
    from app.services.mootdx_service import _trade_days_up_to
    existing = _partition_dates()
    now = now or _dt.datetime.now()
    today = now.date()
    recent = _trade_days_up_to(today)
    if not recent:
        return []
    latest_td = recent[-1]
    if latest_td == today and not _market_closed(now):
        # 今日净值未披露，最新已披露净值属前一交易日
        candidate = recent[-2] if len(recent) >= 2 else latest_td
    else:
        candidate = latest_td
    if existing and _date.fromisoformat(existing[-1]) >= candidate:
        return []
    return [candidate]
