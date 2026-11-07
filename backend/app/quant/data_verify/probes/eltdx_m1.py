"""eltdx（通达信协议）分钟 probe — **可选依赖**，未安装时降级。

``eltdx`` 是第三方 TDX 客户端，只用于核验/回补近 ~94 个交易日的 1m
（design §10），不进主取数链路。本模块的两条纪律：

- ``eltdx`` **只在 :func:`fetch_eltdx_m1` 内部 import**——模块导入、甚至
  :func:`available_sources` 都不许把它拉起来（可选依赖未装时整个核验流程
  仍能跑腾讯/新浪）；
- ``KlineBar.volume_lots`` 单位是**手**（A 股/基金），本地分钟分区存**股**，
  所以解析时 ×100；``amount`` 有就用，缺失才按 ``vol × typical`` 估算。

时间戳：eltdx 默认 ``bar_time="start"``（通达信原始，左端标注），本地
``kline_minute`` 是右端标注（09:30 首根、15:00 末根），所以取数时显式传
``bar_time="end"``，否则整列会左移一分钟、逐 bar 比对全错。
"""

from __future__ import annotations

import importlib.util
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

#: 与 ``kline_minute`` 一致的输出 schema。
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

#: 连接/请求硬墙钟（秒）。
HTTP_TIMEOUT = 10.0

_MARKET_PREFIX = {"SH": "sh", "SZ": "sz", "BJ": "bj"}
_BEIJING = ZoneInfo("Asia/Shanghai")
#: 通达信 ``volume_lots``（手）→ 本地分钟 volume（股）。
LOTS_TO_SHARES = 100.0

__all__ = [
    "HTTP_TIMEOUT",
    "LOTS_TO_SHARES",
    "SCHEMA",
    "available_sources",
    "fetch_eltdx_m1",
    "parse_eltdx_bars",
]


def available_sources() -> list[str]:
    """已安装 ``eltdx`` 时返回 ``["eltdx"]``，否则空列表。

    用 :func:`importlib.util.find_spec` 只做「能否找到模块」的探测，**不 import**。
    """
    return ["eltdx"] if importlib.util.find_spec("eltdx") is not None else []


def _vendor(symbol: str) -> str | None:
    """``600000.SH`` → ``sh600000``；非法/未知市场返回 ``None``。"""
    pure, _, market = symbol.partition(".")
    prefix = _MARKET_PREFIX.get(market.upper())
    if prefix is None or len(pure) != 6 or not pure.isdigit():
        return None
    return f"{prefix}{pure}"


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def _mark_amount_estimated(df: pl.DataFrame) -> pl.DataFrame:
    """``df.attrs["amount"] = "estimated"``（Polars 无该属性时静默降级）。"""
    try:
        attrs = dict(getattr(df, "attrs", None) or {})
        attrs["amount"] = "estimated"
        object.__setattr__(df, "attrs", attrs)
    except (AttributeError, TypeError):
        pass
    return df


def _field(bar: Any, name: str) -> Any:
    """同时兼容 ``KlineBar`` 对象与测试用 dict。"""
    if isinstance(bar, dict):
        return bar.get(name)
    return getattr(bar, name, None)


def _parse_time(raw: Any) -> datetime:
    """``"2026-11-06T15:00:00+08:00"`` / naive datetime → 北京 naive datetime。"""
    dt = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    if dt.tzinfo is not None:
        dt = dt.astimezone(_BEIJING).replace(tzinfo=None)
    return dt


def parse_eltdx_bars(symbol: str, bars: list) -> pl.DataFrame:
    """``KlineBar`` 列表 → 标准分钟表（volume = ``volume_lots × 100`` 股）。

    非法/缺字段的行丢弃；空输入返回空帧。
    """
    rows: list[tuple] = []
    estimated = False
    for bar in bars or []:
        try:
            dt = _parse_time(_field(bar, "time"))
            open_ = float(_field(bar, "open"))
            high = float(_field(bar, "high"))
            low = float(_field(bar, "low"))
            close = float(_field(bar, "close"))
            lots = float(_field(bar, "volume_lots"))
            volume = lots * LOTS_TO_SHARES
            if min(open_, high, low, close) <= 0 or volume < 0:
                continue
            amount_raw = _field(bar, "amount")
            if amount_raw is None:
                amount = volume * (high + low + close) / 3.0
                estimated = True
            else:
                amount = float(amount_raw)
        except (TypeError, ValueError):
            continue
        rows.append((symbol, dt, open_, high, low, close, volume, amount))

    if not rows:
        return _empty()
    df = pl.DataFrame(rows, schema=SCHEMA, orient="row")
    df = df.sort("datetime").unique(
        subset=["datetime"], keep="last", maintain_order=True
    )
    if estimated:
        _mark_amount_estimated(df)
    return df


def fetch_eltdx_m1(
    symbol: str, *, all_pages: bool = True, timeout: float = HTTP_TIMEOUT
) -> pl.DataFrame:
    """用 eltdx 拉单个标的 1m（``all_pages=True`` 实测 ~22560 根 ≈ 94 交易日）。

    未安装 ``eltdx`` 时抛 ``ImportError``（调用方捕获后跳过该源）；网络错误由
    eltdx 自身异常冒泡。北交所/未知市场返回空帧（不触网）。
    """
    vendor = _vendor(symbol)
    if vendor is None:
        return _empty()
    # 延迟导入：模块导入与 available_sources() 都不能依赖这个可选包。
    from eltdx import TdxClient  # type: ignore[import-not-found]

    with TdxClient(timeout=timeout) as client:
        series = client.bars.get(
            vendor, period="1m", all_pages=all_pages, bar_time="end"
        )
    return parse_eltdx_bars(symbol, list(getattr(series, "bars", None) or []))
