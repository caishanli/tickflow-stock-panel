"""新浪分钟 probe（``CN_MarketDataService.getKLineData``）。

接口实测（2026-11-07）：

- ``scale=1&datalen=1023`` 返回最近 1023 根 1m（≈4.2 个交易日），
  ``datalen>1023`` 返回 ``null``；
- 覆盖沪深主板/科创/创业板、ETF 与北交所（``bj920000``）；
- 行字段 ``day/open/high/low/close/volume/amount``，``volume`` 单位是**股**，
  ``amount`` 多数时候有；缺失时按 ``volume × (H+L+C)/3`` 估算，并在
  ``DataFrame.attrs["amount"] = "estimated"`` 打标——成交额永不作为分钟
  修复依据（design §5.1），打标是为了报告里能区分「实测」与「估算」。
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import requests

#: 新浪 1m 单次上限（实测 ``datalen>1023`` 返回 null）。
MAX_COUNT = 1023

_SINA_URL = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}

#: 硬墙钟（秒）：连接 / 读取分别限时。
HTTP_TIMEOUT = 10.0

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

_MARKET_PREFIX = {"SH": "sh", "SZ": "sz", "BJ": "bj"}

__all__ = [
    "HTTP_TIMEOUT",
    "MAX_COUNT",
    "SCHEMA",
    "available_sources",
    "fetch_sina_m1",
    "parse_sina_m1",
]


def available_sources() -> list[str]:
    """本模块可提供的源（runner 汇总用）。"""
    return ["sina"]


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


def _parse_time(raw: object) -> datetime:
    return datetime.fromisoformat(str(raw))


def parse_sina_m1(symbol: str, payload: list) -> pl.DataFrame:
    """新浪 JSON 行 → 标准分钟表（volume 已是股，不换算）。

    ``amount`` 有则直接用；缺失/非法时按 ``volume × (H+L+C)/3`` 估算，并在
    ``df.attrs["amount"]`` 标 ``"estimated"``。非法行丢弃；空输入返回空帧。
    """
    rows: list[tuple] = []
    estimated = False
    for item in payload or []:
        try:
            dt = _parse_time(item["day"])
            open_ = float(item["open"])
            high = float(item["high"])
            low = float(item["low"])
            close = float(item["close"])
            volume = float(item["volume"])
            if min(open_, high, low, close) <= 0 or volume < 0:
                continue
            typical = (high + low + close) / 3.0
            raw_amount = item.get("amount")
            if raw_amount in (None, ""):
                amount = volume * typical
                estimated = True
            else:
                amount = float(raw_amount)
        except (KeyError, TypeError, ValueError):
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


def fetch_sina_m1(
    symbol: str, *, count: int = MAX_COUNT, timeout: float = HTTP_TIMEOUT
) -> pl.DataFrame:
    """拉单个标的最近 ``count`` 根 1m（默认 1023，上限 1023）。

    网络/HTTP/非法响应抛 ``RuntimeError``；``null``/空数组返回空帧。
    """
    if not 0 < count <= MAX_COUNT:
        raise ValueError(f"count 必须在 1..{MAX_COUNT}，收到 {count!r}")
    if timeout <= 0:
        raise ValueError(f"timeout 必须为正数，收到 {timeout!r}")
    vendor = _vendor(symbol)
    if vendor is None:
        return _empty()
    params = {"symbol": vendor, "scale": "1", "ma": "no", "datalen": str(count)}
    try:
        response = requests.get(
            _SINA_URL,
            params=params,
            headers=_HEADERS,
            timeout=(min(5.0, timeout), timeout),
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise RuntimeError(
            f"新浪 1m 请求 {vendor} 失败: {type(exc).__name__}: {exc}"
        ) from exc
    if payload is None:
        return _empty()
    if not isinstance(payload, list):
        raise RuntimeError(f"新浪 1m 响应不是数组（{vendor}）: {type(payload).__name__}")
    return parse_sina_m1(symbol, payload)
