"""实时快照源：腾讯/新浪批量 HTTP 拉取 + 统一归一化 RTQuote。

设计 spec：docs/superpowers/specs/2026-08-25-stockdata-rt-tencent-sina-design.md。
单位口径（实测校准 2026-08-25）：RTQuote.cum_volume 一律「股」、cum_amount 一律「元」；
腾讯原始量=手(×100)、额=万元(×1e4)；新浪量=股、额=元直用。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import re
from dataclasses import dataclass
from typing import ClassVar

import polars as pl
import requests

logger = logging.getLogger("app.services.stockdata.rt_sources")

_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
_TENCENT_URL = "https://qt.gtimg.cn/q="
_SINA_URL = "https://hq.sinajs.cn/list="
_HTTP_TIMEOUT = 3.0
# 单请求标的上限：实测腾讯 6870 只单 URL(61KB) 会吃 HTTP 414；200 只/块
# 全市场 35 请求 0.5s 内拉完且无限流（2026-08-26 盘前实测）。env 可调。
try:
    RT_BATCH_SIZE = max(1, int(os.getenv("STOCKDATA_RT_BATCH", "") or 200))
except ValueError:
    RT_BATCH_SIZE = 200


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
            cum_amount=_f(v[37]) * 1e4,
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
        out: dict[str, RTQuote] = {}
        try:
            timeout = float(os.getenv("STOCKDATA_RT_HTTP_TIMEOUT", "") or _HTTP_TIMEOUT)
            for i in range(0, len(vendor), RT_BATCH_SIZE):
                resp = self._session.get(
                    self._url + ",".join(vendor[i:i + RT_BATCH_SIZE]), timeout=timeout)
                resp.raise_for_status()
                resp.encoding = "gbk"
                out.update(self._parser(resp.text))
        except Exception as e:  # noqa: BLE001
            # 已成功块的结果保留（部分数据优于全丢）；缺失标的由编排链降级下家
            logger.warning("[rt_sources] %s 批量拉取失败(%s 只): %s",
                           type(self).__name__, len(vendor), e)
        return out

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


_MINUTE_COLS = ["symbol", "datetime", "open", "high", "low", "close",
                "volume", "amount"]


class _SymState:
    __slots__ = ("amt", "bar_dt", "c", "h", "l",
                 "last_amt", "last_price", "last_vol", "o", "v")

    def __init__(self) -> None:
        self.last_price = 0.0
        self.last_vol = 0.0
        self.last_amt = 0.0
        self.bar_dt: _dt.datetime | None = None
        self.o = self.h = self.l = self.c = 0.0
        self.v = self.amt = 0.0


def _floor_minute(ts: _dt.datetime) -> _dt.datetime:
    return ts.replace(second=0, microsecond=0)


class BarSynthesizer:
    """连续快照 → 分钟 bar（线程安全；per-symbol 状态仅 watchlist 规模内存）。

    update 返回：本次封口的历史 bar 行 + 各标的当前分钟 bar 最新整行。
    调用方按 (symbol, datetime) upsert 幂等合并。
    """

    def __init__(self) -> None:
        import threading
        self._lock = threading.Lock()
        self._state: dict[str, _SymState] = {}
        self._quote_ts: dict[str, _dt.datetime] = {}
        self._day: _dt.date = _dt.date.today()

    def reset_if_new_day(self, day: _dt.date) -> None:
        with self._lock:
            if day != self._day:
                self._state.clear()
                self._quote_ts.clear()
                self._day = day

    def last_quote_time(self, symbol: str) -> _dt.datetime | None:
        with self._lock:
            return self._quote_ts.get(symbol)

    def update(self, quotes: dict[str, RTQuote],
               now: _dt.datetime | None = None) -> list[pl.DataFrame]:
        now = now or _dt.datetime.now()
        done_rows: list[tuple] = []      # 跨分钟封口的旧 bar
        cur_rows: list[tuple] = []       # 每标的当前 bar 最新整行
        with self._lock:
            for sym, q in quotes.items():
                if q.price <= 0:
                    continue
                st = self._state.get(sym)
                if st is None:
                    st = _SymState()
                    self._state[sym] = st
                has_bar = st.bar_dt is not None
                dv = max(q.cum_volume - st.last_vol, 0.0) if has_bar else 0.0
                da = max(q.cum_amount - st.last_amt, 0.0) if has_bar else 0.0
                bar_dt = _floor_minute(q.quote_time or now)
                if not has_bar:
                    # 首拍：只建价格 bar，量额记 0（保守不虚增）
                    st.o = st.h = st.l = st.c = q.price
                    st.v = st.amt = 0.0
                    st.bar_dt = bar_dt
                elif st.bar_dt is not None and bar_dt > st.bar_dt:
                    # 封口旧 bar → done；开新 bar：上一拍价格开盘
                    # （仅向前滚动：迟到旧分钟 tick 走 else 并入当前 bar，不重开封口 bar）
                    done_rows.append((sym, st.bar_dt, st.o, st.h, st.l,
                                      st.c, st.v, st.amt))
                    st.o = st.c
                    st.h = max(st.c, q.price)
                    st.l = min(st.c, q.price)
                    st.c = q.price
                    st.v = dv
                    st.amt = da
                    st.bar_dt = bar_dt
                else:
                    # 同分钟（含迟到旧分钟 tick 折入当前 bar）：更新 HLC、量额差分累加
                    st.h = max(st.h, q.price)
                    st.l = min(st.l, q.price)
                    st.c = q.price
                    st.v += dv
                    st.amt += da
                st.last_price = q.price
                st.last_vol = q.cum_volume
                st.last_amt = q.cum_amount
                self._quote_ts[sym] = q.quote_time or now
                cur_rows.append((sym, st.bar_dt, st.o, st.h, st.l,
                                 st.c, st.v, st.amt))
        if not cur_rows and not done_rows:
            return []
        all_rows = done_rows + cur_rows
        cols = list(zip(*all_rows, strict=True))
        df = pl.DataFrame({
            "symbol": list(cols[0]), "datetime": list(cols[1]),
            "open": list(cols[2]), "high": list(cols[3]), "low": list(cols[4]),
            "close": list(cols[5]), "volume": list(cols[6]),
            "amount": list(cols[7]),
        }, schema_overrides={"datetime": pl.Datetime("us")})
        return [df]
