"""归档 zip → 本机分区 parquet 的导入转换。

归档结构（/home/caisl/A股分钟数据/4.21更新/...）：
  1m_price/2019.zip  → 内含 20190102.parquet ... 20191231.parquet（按交易日）
  1d_price/2019.zip  → 内含 20190102.parquet ...（按交易日，日线多 adj_factor/pre_close 等列）

本机分区格式（data/kline_minute/date=YYYY-MM-DD/part.parquet）：
  kline_minute: symbol(str), datetime(datetime[us]), open/high/low/close(volume/amount)(f64)
  kline_daily : symbol(str), date(date), open/high/low/close/volume/amount(f64)

已实测（2026-10-20，见 docs/2026-10-20-archive-import-feasibility.md）：
  - 价格 4 列（OHLC）在重叠日与本机**逐行零差异** → 复权口径一致，可直接导入
  - 归档 1m 的 vol 单位 = 股，与本机 kline_minute 一致（0 差异）
  - 归档 1d 的 vol 单位 = 股，本机 kline_daily 存「手」→ 需 ÷100
  - amount 差异 ≤ 1 元（浮点精度），无影响
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import polars as pl

# 归档 1m/1d 的列名 → 本机列名
MINUTE_COLUMNS = {
    "code": "symbol",
    "trade_time": "datetime",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "vol": "volume",
    "amount": "amount",
}
DAILY_COLUMNS = {
    "code": "symbol",
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "vol": "volume",
    "amount": "amount",
}

# 本机目标 schema（顺序即列顺序）
MINUTE_SCHEMA = [
    ("symbol", pl.String),
    ("datetime", pl.Datetime("us")),
    ("open", pl.Float64),
    ("high", pl.Float64),
    ("low", pl.Float64),
    ("close", pl.Float64),
    ("volume", pl.Float64),
    ("amount", pl.Float64),
]
DAILY_SCHEMA = [
    ("symbol", pl.String),
    ("date", pl.Date),
    ("open", pl.Float64),
    ("high", pl.Float64),
    ("low", pl.Float64),
    ("close", pl.Float64),
    ("volume", pl.Float64),
    ("amount", pl.Float64),
]


def _rename_and_cast(df: pl.DataFrame, mapping: dict[str, str], schema: list) -> pl.DataFrame:
    """按 mapping 改名，按 schema 选列并转 dtype。丢弃 schema 外的列。"""
    df = df.rename(mapping)
    df = df.select([name for name, _ in schema])
    casts = []
    for name, dtype in schema:
        cur = df.schema[name]
        if cur == dtype:
            continue
        if dtype == pl.Datetime("us") and cur == pl.String:
            casts.append(pl.col(name).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.cast_time_unit("us"))
        elif dtype == pl.Date and cur == pl.String:
            casts.append(pl.col(name).str.to_date("%Y%m%d"))
        elif dtype == pl.Float64 and cur.is_float():
            casts.append(pl.col(name).cast(pl.Float64))
        else:
            casts.append(pl.col(name).cast(dtype))
    if casts:
        df = df.with_columns(casts)
    return df


def convert_minute(df: pl.DataFrame) -> pl.DataFrame:
    """归档 1m → 本机 kline_minute 格式。volume 单位一致（股），不换算。"""
    return _rename_and_cast(df, MINUTE_COLUMNS, MINUTE_SCHEMA)


def convert_daily(df: pl.DataFrame) -> pl.DataFrame:
    """归档 1d → 本机 kline_daily 格式。归档 vol 是「股」，本机存「手」→ ÷100。

    注意这是**股票**口径。ETF 目标表（kline_etf_daily）必须走
    :func:`convert_daily_etf`：ETF 存盘即「股」，再 ÷100 就会写坏。
    """
    out = _rename_and_cast(df, DAILY_COLUMNS, DAILY_SCHEMA)
    return out.with_columns((pl.col("volume") / 100).alias("volume"))


def convert_daily_etf(df: pl.DataFrame) -> pl.DataFrame:
    """归档 1d → 本机 kline_etf_daily 格式。归档 vol 是「股」，ETF 存盘也是「股」→ 不换算。"""
    return _rename_and_cast(df, DAILY_COLUMNS, DAILY_SCHEMA)


def iter_zip_days(zip_path: Path) -> list[str]:
    """列出 zip 内按交易日命名的 parquet 成员名（YYYYMMDD.parquet），已排序。"""
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith(".parquet") and "/" not in n]
    return sorted(names)


def read_day_from_zip(zip_path: Path, member: str) -> pl.DataFrame:
    """从 zip 里读出某个交易日的 parquet（解压到内存，不落盘）。"""
    with zipfile.ZipFile(zip_path) as zf, zf.open(member) as fh:
        return pl.read_parquet(fh)


def day_to_partition(member: str) -> str:
    """YYYYMMDD.parquet → YYYY-MM-DD"""
    stem = Path(member).stem
    return f"{stem[:4]}-{stem[4:6]}-{stem[6:8]}"
