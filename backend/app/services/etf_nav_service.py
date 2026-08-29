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
    """返回 akshare 快照中**已实质披露**的最新披露日。

    akshare 对当日未披露净值的基金填 ``---`` 占位：若直接取最新列名（如盘中
    08-14 列全为占位），会过早写入空/稀疏分区且幂等永不重补。规则：取有效行数
    >= 全列最大有效行数 50% 的**最新**日期列——当日未披露（占位多、有效行远低于
    前一完整日）时自然落到前一披露日；当日披露过半（如晚间 1471/1601）则推进
    到当日。全部无有效值返回 None（调用方跳过落盘）。
    """
    counts: dict[_date, int] = {}
    for col in raw.columns:
        m = _NAV_DATE_COL_RE.match(str(col))
        if not m:
            continue
        n = raw.filter(pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars()
                       .str.replace("---", "", literal=True)
                       .str.contains(r"^-?\d+(\.\d+)?$", strict=False)).height
        counts[_date.fromisoformat(m.group(1))] = n
    if not counts:
        return None
    max_count = max(counts.values())
    if max_count <= 0:
        return None
    threshold = max_count * 0.5
    candidates = [d for d, n in counts.items() if n >= threshold]
    return max(candidates) if candidates else None


def sync_etf_nav(day: _date | None = None) -> int:
    """同步全市场 ETF 单位净值到按日分区。幂等。

    分区日期 = 净值实际披露日：从 akshare 列名解析（最新 ``YYYY-MM-DD-单位净值``
    列的日期），``day`` 仅在解析失败时兜底（默认今天）。某日分区已存在返回 0。
    """
    fallback = day or _date.today()
    raw = _fund_etf_fund_daily_em()
    if raw.is_empty():
        # 第3层: 空结果重试一次(akshare 偶发抖动), 仍空才记录失败
        raw = _fund_etf_fund_daily_em()
    if raw.is_empty():
        from app.services.mootdx_service import _append_failure
        _append_failure(f"etf_nav:{fallback}", "empty_akshare")
        return 0
    nav_date = _nav_date_from_columns(raw)
    if nav_date is None:
        # akshare 快照无任何有效净值（如盘初/数据源异常）：不落盘，留给后续重试
        from app.services.mootdx_service import _append_failure
        _append_failure(f"etf_nav:{fallback}", "no_valid_nav")
        return 0
    unit_col = f"{nav_date.isoformat()}-单位净值"
    if unit_col not in raw.columns:
        from app.services.mootdx_service import _append_failure
        _append_failure(f"etf_nav:{nav_date}", "no_nav_column")
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
    if df.is_empty():
        # 有效净值 0 行：不写空/稀疏分区
        logger.warning("etf_nav_service: %s 有效净值 0 行，跳过落盘", nav_date)
        return 0
    pdir = ETF_NAV_ROOT / f"date={nav_date}"
    part = pdir / "part.parquet"
    if part.exists():
        try:
            n_existing = pl.read_parquet(part, columns=["symbol"]).height
        except Exception:
            n_existing = 0
        # 已有分区不稀疏（不比本次快照少）→ 幂等跳过；稀疏（过早写入的占位）→ 覆盖重写
        if n_existing > 0 and n_existing >= df.height:
            logger.info("etf_nav_service: %s 已存在（%d 行），跳过", nav_date, n_existing)
            return 0
        logger.warning("etf_nav_service: %s 分区稀疏（%d 行 < 本次 %d 行），覆盖重写",
                       nav_date, n_existing, df.height)
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


def _latest_valid_nav_date() -> _date | None:
    """最新"有效"净值分区日期：分区 parquet 存在且行数 > 0。

    空/稀疏分区（过早写入的占位数据）不视为已覆盖，否则会掩盖真实缺失。
    """
    latest: _date | None = None
    for ds in _partition_dates():
        part = ETF_NAV_ROOT / f"date={ds}" / "part.parquet"
        if not part.exists():
            continue
        try:
            n = pl.read_parquet(part, columns=["symbol"]).height
        except Exception:
            n = 0
        if n > 0:
            d = _date.fromisoformat(ds)
            if latest is None or d > latest:
                latest = d
    return latest


def _missing_etf_nav_days(now: _dt.datetime | None = None) -> list[_date]:
    """返回需回源的净值交易日：**最多一个**（最新缺失日，历史不补）。

    akshare ``fund_etf_fund_daily_em`` 只有当前快照（无逐日历史），回补历史
    只会把今日净值写成过去每一天（假数据）。因此本函数只返回最新应披露净值的
    交易日中缺失的一个：
    - 交易日当天盘中（<15:00）当日净值未披露，最新已披露净值属前一交易日
      → 候选 = 前一交易日（00:00 巡检即落此分支）；
    - 收盘后（≥15:00）当日净值已披露 → 候选 = 最新交易日；
    - 非交易日（周末/节假日）→ 候选 = 最近一个交易日；
    - 已有**有效**分区日期 >= 候选 → 无需回源（空/稀疏分区不算覆盖）。
    """
    from app.services.mootdx_service import _trade_days_up_to
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
    covered = _latest_valid_nav_date()
    if covered is not None and covered >= candidate:
        return []
    return [candidate]
