"""实时快照源：腾讯/新浪批量 HTTP 拉取 + 统一归一化 RTQuote。

设计 spec：docs/superpowers/specs/2026-08-25-stockdata-rt-tencent-sina-design.md。
单位口径（实测校准 2026-08-25）：RTQuote.cum_volume 一律「股」、cum_amount 一律「元」；
腾讯原始量=手(×100)、额=元直用（实测 field37 与新浪同源同量级，非万元）；新浪量=股、额=元直用。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import re
from dataclasses import dataclass
from typing import ClassVar

import requests

logger = logging.getLogger("app.services.stockdata.rt_sources")

_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
_TENCENT_URL = "https://qt.gtimg.cn/q="
_SINA_URL = "https://hq.sinajs.cn/list="
_HTTP_TIMEOUT = 3.0


@dataclass
class RTQuote:
    symbol: str
    price: float
    prev_close: float
    open_: float
    high: float
    low: float
    cum_volume: float
    cum_amount: float
    quote_time: _dt.datetime


def _f(v: str) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _tf_to_vendor(symbol: str) -> str | None:
    pure, _, suf = symbol.rpartition(".")
    if not pure:
        return None
    if suf == "SH":
        return f"sh{pure}"
    if suf == "SZ":
        return f"sz{pure}"
    return None


def _parse_ts(raw: str) -> _dt.datetime:
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return _dt.datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return _dt.datetime.now()


def parse_tencent_payload(text: str) -> dict[str, RTQuote]:
    """腾讯 v_shXXXXXX="1~名称~code~现价~昨收~今开~量(手)~..." ~分隔 payload 解析。"""
    out: dict[str, RTQuote] = {}
    for m in re.finditer(r'v_(?:sh|sz)(\d{6})="([^"]*)"', text):
        code, body = m.group(1), m.group(2)
        v = body.split("~")
        if len(v) < 38:
            continue
        price = _f(v[3])
        if price <= 0:
            continue
        suffix = "SH" if m.group(0).startswith("v_sh") else "SZ"
        out[f"{code}.{suffix}"] = RTQuote(
            symbol=f"{code}.{suffix}",
            price=price,
            prev_close=_f(v[4]),
            open_=_f(v[5]),
            high=_f(v[33]),
            low=_f(v[34]),
            cum_volume=_f(v[6]) * 100.0,
            cum_amount=_f(v[37]),
            quote_time=_parse_ts(v[30]),
        )
    return out


def parse_sina_payload(text: str) -> dict[str, RTQuote]:
    """新浪 var hq_str_shXXXXXX="名,今开,昨收,现价,最高,最低,...,量(股),额(元),...,日期,时间" 解析。"""
    out: dict[str, RTQuote] = {}
    for m in re.finditer(r'hq_str_(?:sh|sz)(\d{6})="([^"]*)"', text):
        code, body = m.group(1), m.group(2)
        v = body.split(",")
        if len(v) < 32:
            continue
        price = _f(v[3])
        if price <= 0:
            continue
        suffix = "SH" if m.group(0).startswith("hq_str_sh") else "SZ"
        out[f"{code}.{suffix}"] = RTQuote(
            symbol=f"{code}.{suffix}",
            price=price,
            prev_close=_f(v[2]),
            open_=_f(v[1]),
            high=_f(v[4]),
            low=_f(v[5]),
            cum_volume=_f(v[8]),
            cum_amount=_f(v[9]),
            quote_time=_parse_ts(f"{v[30]} {v[31]}"),
        )
    return out


class _HttpSource:
    _url: ClassVar[str]
    _parser: ClassVar[staticmethod[[str], dict[str, RTQuote]]]
    _headers: ClassVar[dict[str, str]]

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(self._headers)

    def fetch(self, symbols: list[str]) -> dict[str, RTQuote]:
        vendor = [s for s in (_tf_to_vendor(x) for x in symbols) if s]
        if not vendor:
            return {}
        try:
            resp = self._session.get(
                self._url + ",".join(vendor),
                timeout=float(os.getenv("STOCKDATA_RT_HTTP_TIMEOUT", "") or _HTTP_TIMEOUT))
            resp.raise_for_status()
            resp.encoding = "gbk"
            return self._parser(resp.text)
        except Exception as e:  # noqa: BLE001
            logger.warning("[rt_sources] %s 批量拉取失败(%s 只): %s",
                           type(self).__name__, len(vendor), e)
            return {}

    def close(self) -> None:
        self._session.close()


class TencentRTSource(_HttpSource):
    _url = _TENCENT_URL
    _parser = staticmethod(parse_tencent_payload)
    _headers: ClassVar[dict[str, str]] = dict(_UA)


class SinaRTSource(_HttpSource):
    _url = _SINA_URL
    _parser = staticmethod(parse_sina_payload)
    _headers: ClassVar[dict[str, str]] = {**_UA, "Referer": "https://finance.sina.com.cn"}
