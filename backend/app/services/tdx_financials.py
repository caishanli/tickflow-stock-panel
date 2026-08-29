"""通达信专业财务数据（mootdx affair / gpcw）回源落盘。

每季度一个 ``gpcwYYYYMMDD.zip`` 覆盖全市场（~5500 只 × ~585 列），
下载解析后仅保留策略查询需要的列子集，按报告期分区落盘：

    data/financials/tdx/stat=YYYYMMDD/part.parquet

Point-in-time 防前视：TDX 无公告日(pubDate)，用法定披露窗口保守近似——
季报仅在「报告期对应的法定披露截止日」之后可见，预计算为 ``visible_from``
列；查询侧按 ``previous_date >= visible_from`` 取最新一条。
"""

from __future__ import annotations

import datetime as _dt
import logging
import os
import shutil
import tempfile
import threading
from pathlib import Path

import polars as pl

logger = logging.getLogger("app.services.tdx_financials")

_env_root = os.getenv("PARTITION_DATA_ROOT", "").strip()
if _env_root:
    DATA_ROOT = Path(_env_root)
else:
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))
    DATA_ROOT = Path(_repo_root) / "data"

FINANCIALS_ROOT = DATA_ROOT / "financials" / "tdx"

# TDX 列名 → 本库标准列名。注意 gpcw 有重名列（如两个「净资产收益率」），
# 提取时取首个匹配。金额单位：TDX 绝对额列为元；营业总收入为万元(×1e4)。
_COLUMN_MAP: list[tuple[str, str]] = [
    ("五、净利润", "net_profit"),
    ("归属于母公司所有者的净利润", "np_parent_company_owners"),
    ("营业总收入(万元)", "operating_revenue_wan"),
    ("经营活动现金流入小计", "subtotal_operate_cash_inflow"),
    ("扣除非经常性损益后的净利润", "adjusted_profit"),
    ("净资产收益率", "roe"),
    ("资产总计", "total_assets"),
    ("每股净资产", "bps"),
    ("总股本", "total_shares"),
    ("已上市流通A股", "float_a_shares"),
]

_SYNC_LOCK = threading.Lock()


def _report_periods(count: int, today: _dt.date | None = None) -> list[str]:
    """最近 count 个报告期（含当期未披露完的），YYYYMMDD 列表，新→旧。"""
    today = today or _dt.date.today()
    y, m = today.year, today.month
    out: list[str] = []
    while len(out) < count:
        for md in ("1231", "0930", "0630", "0331"):
            period = f"{y}{md}"
            if int(period) <= int(today.strftime("%Y%m%d")) and period not in out:
                out.append(period)
                if len(out) >= count:
                    break
        y -= 1
    return sorted(out)


def visible_from(stat_yyyymmdd: str) -> str:
    """法定披露截止日：Q1→当年4/30，中报→8/31，三季报→10/31，年报→次年4/30。"""
    y = int(stat_yyyymmdd[:4])
    md = stat_yyyymmdd[4:]
    if md == "0331":
        return f"{y}-04-30"
    if md == "0630":
        return f"{y}-08-31"
    if md == "0930":
        return f"{y}-10-31"
    if md == "1231":
        return f"{y + 1}-04-30"
    raise ValueError(f"非法报告期: {stat_yyyymmdd}")


def _extract(df, stat: str) -> pl.DataFrame:
    """从 gpcw 解析结果（pandas，code 为索引，可能含重名列）提取所需列。"""
    import pandas as pd

    pdf = df.reset_index()
    data: dict[str, pd.Series] = {"code": pdf["code"].astype(str)}
    for tdx_name, std_name in _COLUMN_MAP:
        if tdx_name not in pdf.columns:
            logger.warning("gpcw %s 缺列: %s", stat, std_name)
            continue
        s = pdf.loc[:, tdx_name]
        if isinstance(s, pd.DataFrame):  # 重名列取首个
            s = s.iloc[:, 0]
        data[std_name] = pd.to_numeric(s, errors="coerce")
    out = pl.DataFrame(data)
    if "operating_revenue_wan" in out.columns:
        out = out.with_columns((pl.col("operating_revenue_wan") * 1e4)
                               .alias("operating_revenue"))
        out = out.drop("operating_revenue_wan")
    out = out.with_columns([
        pl.col("code").cast(pl.Utf8).str.strip_chars().str.slice(0, 6).alias("symbol"),
        pl.lit(stat).alias("stat_date"),
        pl.lit(visible_from(stat)).alias("visible_from"),
    ])
    num_cols = [c for c in out.columns if c not in ("symbol", "stat_date", "visible_from")]
    out = out.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in num_cols])
    std_order = ["net_profit", "np_parent_company_owners", "operating_revenue",
                 "subtotal_operate_cash_inflow", "adjusted_profit", "roe",
                 "total_assets", "bps", "total_shares", "float_a_shares"]
    keep = [c for c in std_order if c in out.columns]
    return out.select(["symbol", "stat_date", "visible_from"] + keep)


def sync_financials(quarters: int = 8, force: bool = False) -> dict:
    """回源最近 quarters 个报告期的财务文件，幂等（已有分区跳过）。

    返回 {"synced": [...], "skipped": [...], "failed": {...}}。
    """
    with _SYNC_LOCK:
        from mootdx.affair import Affair

        stats = _report_periods(quarters)
        res: dict = {"synced": [], "skipped": [], "failed": {}}
        tmpdir = tempfile.mkdtemp(prefix="gpcw_")
        try:
            for stat in stats:
                part_dir = FINANCIALS_ROOT / f"stat={stat}"
                part_file = part_dir / "part.parquet"
                if part_file.exists() and not force:
                    res["skipped"].append(stat)
                    continue
                filename = f"gpcw{stat}.zip"
                try:
                    Affair.fetch(downdir=tmpdir, filename=filename)
                    df = Affair.parse(downdir=tmpdir, filename=filename)
                    if df is None or getattr(df, "empty", not len(df)):
                        raise RuntimeError("解析结果为空")
                    out = _extract(df, stat)
                    part_dir.mkdir(parents=True, exist_ok=True)
                    tmp = part_file.with_suffix(".tmp.parquet")
                    out.write_parquet(tmp)
                    os.replace(tmp, part_file)
                    res["synced"].append(stat)
                    logger.info("financials: %s 落盘 %s 行", stat, out.height)
                except Exception as exc:
                    logger.warning("financials: %s 回源失败: %s", stat, exc)
                    res["failed"][stat] = str(exc)[:200]
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        return res


def load_financials() -> pl.DataFrame:
    """读全部财务分区（服务端 get_financials 用）。无数据返回空表。"""
    if not FINANCIALS_ROOT.is_dir():
        return pl.DataFrame()
    parts = sorted(FINANCIALS_ROOT.glob("stat=*/part.parquet"))
    if not parts:
        return pl.DataFrame()
    return pl.read_parquet(parts)
