"""腾讯 ``mkline`` 分钟 probe（核验用，复用生产回源的 URL/解析口径）。

对外只暴露「取一份标准分钟表」的能力，不做判定、不写盘：

- :func:`parse_tencent_m1`：把腾讯 ``m1`` 行解析成
  ``symbol, datetime, open, high, low, close, volume, amount``；
  单位复用 :func:`app.services.tencent_minute.parse_m1_rows`——主板/创业板/ETF
  原始量是「手」、科创板 688/689 是「股」，统一折成**股**（与 ``kline_minute``
  契约一致），避免核验侧再犯一次 ×100。
- :func:`fetch_tencent_m1`：单个标的近 ~800 根 1m（≈3.3 个交易日；腾讯没有
  日期参数，超过 800 根直接退化到 320）。

网络失败（连接/HTTP/JSON 异常）抛 ``RuntimeError``，空数据（停牌/限流）返回
空帧——调用方据此区分「源不可用」与「该标的当天无 bar」。
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import requests

from app.services.tencent_minute import fetch_m1, parse_m1_rows, tf_to_vendor

#: 与 ``kline_minute`` 一致的输出 schema（volume 股、amount 元）。
SCHEMA: dict[str, pl.DataType] = {
    "symbol": pl.String(),
    "datetime": pl.Datetime("us"),
    "open": pl.Float64(),
    "high": pl.Float64(),
    "low": pl.Float64(),
    "close": pl.Float64(),
    "volume": pl.Float64(),
    "amount": pl.Float64(),
}

__all__ = [
    "SCHEMA",
    "available_sources",
    "fetch_tencent_m1",
    "parse_tencent_m1",
]


def available_sources() -> list[str]:
    """本模块可提供的源（runner 汇总用；HTTP 源不依赖可选包）。"""
    return ["tencent"]


def _row_days(rows: list) -> set[date]:
    """``days=None`` 时从行内时间戳推出去重日期集合（非法行忽略）。"""
    days: set[date] = set()
    for row in rows:
        try:
            days.add(datetime.strptime(str(row[0]), "%Y%m%d%H%M").date())
        except (TypeError, ValueError, IndexError):
            continue
    return days


def parse_tencent_m1(
    symbol: str, rows: list, days: set[date] | None = None
) -> pl.DataFrame:
    """腾讯 ``m1`` 行 → 标准分钟表（volume 股；amount 按 ``vol × typical`` 估算）。

    ``days=None`` 表示不过滤（保留全部 bar）；给了日期集合时只保留其中日期
    的 bar。解析与单位规则全部复用生产侧的 :func:`parse_m1_rows`，本函数只
    负责补上「不传 days」这一核验侧常用默认值。
    """
    selected = _row_days(rows) if days is None else {d for d in days}
    return parse_m1_rows(symbol, rows, selected)


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def fetch_tencent_m1(
    symbol: str, *, days: set[date] | None = None
) -> pl.DataFrame:
    """拉单个标的的腾讯 1m（~800 根），可选按 ``days`` 过滤。

    北交所（``.BJ``）腾讯没有数据，直接返回空帧（不触网）。
    网络/HTTP/JSON 失败抛 ``RuntimeError``；源返回空列表时返回空帧。
    单请求硬墙钟沿用生产侧 :data:`app.services.tencent_minute._HTTP_TIMEOUT`
    （10s），不在这里另开一套超时参数。
    """
    vendor = tf_to_vendor(symbol)
    if vendor is None:
        return _empty()
    with requests.Session() as session:
        rows = fetch_m1(session, vendor)
    if rows is None:
        raise RuntimeError(f"腾讯 mkline 请求 {vendor} 失败（网络/HTTP/JSON）")
    return parse_tencent_m1(symbol, rows, days=days)
