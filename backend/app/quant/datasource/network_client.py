"""jqdata 风格网络行情客户端（stock data 服务唯一取数入口）。

量化侧（回测/模拟盘）只能经此类取数：不读本地 parquet、不直连 mootdx/astock。
返回帧均为 DatetimeIndex + OHLCV/volume/amount(+trade_dt)，按 jq 代码分帧。
"""
from __future__ import annotations

import io
import itertools
import logging
import os
import socket
import threading
import time

import msgpack
import pandas as pd
import polars as pl

log = logging.getLogger("app.quant.datasource.network_client")

_HEADER = 4


def _default_host() -> str:
    return os.getenv("STOCKDATA_HOST", "127.0.0.1")


def _default_port() -> int:
    try:
        return int(os.getenv("STOCKDATA_PORT", "") or 3322)
    except (TypeError, ValueError):
        return 3322


def _to_jq(code: str) -> str:
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".XSHG" if suf in ("SH", "SS", "XSHG") else ".XSHE")


class StockDataClient:
    def __init__(self, host: str | None = None, port: int | None = None,
                 timeout: float = 120.0, connect_timeout: float = 5.0):
        self.host = host or _default_host()
        self.port = port or _default_port()
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self._ids = itertools.count(1)
        self._sock: socket.socket | None = None
        self._sock_lock = threading.Lock()

    # ---- 连接管理 ----
    def _connect(self) -> socket.socket:
        s = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        s.settimeout(self.timeout)
        self._sock = s
        return s

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("连接已关闭")
            buf.extend(chunk)
        return bytes(buf)

    def _request(self, method: str, params: dict, retry: int = 3):
        payload = msgpack.packb(
            {"v": 1, "id": next(self._ids), "m": method, "p": params},
            use_bin_type=True)
        frame = len(payload).to_bytes(_HEADER, "big") + payload
        last: Exception | None = None
        resp: dict | None = None
        for attempt in range(retry):
            with self._sock_lock:
                try:
                    if self._sock is None:
                        self._connect()
                    self._sock.sendall(frame)
                    n = int.from_bytes(self._recv_exact(_HEADER), "big")
                    resp = msgpack.unpackb(self._recv_exact(n), raw=False)
                    break
                except Exception as e:  # noqa: BLE001
                    last = e
                    try:
                        if self._sock is not None:
                            self._sock.close()
                    except Exception:
                        pass
                    self._sock = None
            if attempt < retry - 1:
                time.sleep(min(0.5 * (2 ** attempt), 5.0))
        if resp is None:
            raise ConnectionError(f"stock data 服务不可达 ({self.host}:{self.port}): {last}")
        if not resp.get("ok"):
            d = resp.get("d") or {}
            raise RuntimeError(f"{method} 失败: {d.get('msg')} ({d.get('code')})")
        return resp

    def _parquet_to_dict(self, resp: dict) -> dict[str, pd.DataFrame]:
        """parquet 响应 → {jq_code: DatetimeIndex df}。"""
        if resp["t"] != "parquet":
            return {}
        raw = resp["d"]
        if not raw:
            return {}
        df = pl.read_parquet(io.BytesIO(raw))
        if df.is_empty():
            return {}
        pdf = df.to_pandas()
        has_date = "date" in pdf.columns
        ts_col = "date" if has_date else "datetime"
        pdf = pdf.set_index(pd.to_datetime(pdf[ts_col]))
        pdf.index.name = None
        drop = ["symbol", ts_col]
        out: dict[str, pd.DataFrame] = {}
        for sym, g in pdf.groupby("symbol"):
            sub = g.drop(columns=[c for c in drop if c in g.columns]).copy()
            if has_date:
                sub["trade_dt"] = pd.to_datetime(g.index.normalize()).values
            # 服务端分区数据本就按 symbol+date 升序（T17 实测 per-symbol 恒单调），
            # 已排序帧跳过 sort_index（7196 组 × 排序 ≈ 秒级）；非单调时仍兜底排。
            if not sub.index.is_monotonic_increasing:
                sub = sub.sort_index()
            out[_to_jq(sym)] = sub
        return out

    # ---- 行情 ----
    def get_price(self, security, start_date=None, end_date=None, frequency="daily",
                  fields=None) -> dict[str, pd.DataFrame]:
        resp = self._request("get_price", {
            "security": security, "start_date": str(start_date) if start_date else None,
            "end_date": str(end_date) if end_date else None,
            "frequency": frequency, "fields": fields})
        return self._parquet_to_dict(resp)

    def get_minute_pool(self, codes, lo_ts, hi_ts) -> dict[str, pd.DataFrame]:
        resp = self._request("get_minute", {
            "security": list(codes),
            "lo_ts": str(lo_ts) if lo_ts is not None else None,
            "hi_ts": str(hi_ts) if hi_ts is not None else None})
        return self._parquet_to_dict(resp)

    def current_snapshot(self, codes, as_of=None) -> dict[str, pd.DataFrame]:
        resp = self._request("current_snapshot", {
            "security": list(codes),
            "as_of": str(as_of) if as_of is not None else None})
        return self._parquet_to_dict(resp)

    def preload_daily(self, lookback_days: int = 400, asof=None) -> dict[str, pd.DataFrame]:
        resp = self._request("preload_daily", {
            "lookback_days": lookback_days,
            "asof": str(asof) if asof is not None else None})
        return self._parquet_to_dict(resp)

    def get_adj_factors(self) -> pd.DataFrame:
        resp = self._request("get_adj_factors", {})
        if resp["t"] != "parquet" or not resp["d"]:
            return pd.DataFrame()
        return pl.read_parquet(io.BytesIO(resp["d"])).to_pandas()

    def get_etf_nav(self, codes, date=None) -> dict[str, pd.DataFrame]:
        resp = self._request("get_etf_nav", {
            "security": codes, "date": date,
        })
        return self._parquet_to_dict(resp)

    # ---- 列表/元数据 ----
    def get_trade_days(self, start_date, end_date) -> list[str]:
        return self._request("get_trade_days", {
            "start_date": str(start_date), "end_date": str(end_date)})["d"]

    def get_all_securities(self, types=None, date=None) -> pd.DataFrame:
        resp = self._request("get_all_securities", {"types": types, "date": date})
        if resp["t"] != "parquet" or not resp["d"]:
            return pd.DataFrame()
        return pl.read_parquet(io.BytesIO(resp["d"])).to_pandas()

    def get_security_info(self, code) -> dict:
        return self._request("get_security_info", {"code": code})["d"]

    def get_index_stocks(self, index_code, date=None) -> list[str]:
        return self._request("get_index_stocks", {"index_code": index_code, "date": date})["d"]

    def get_stock_names(self, codes=None) -> dict:
        return self._request("get_stock_names", {"codes": codes})["d"]

    # ---- 运维 ----
    def ping(self) -> dict:
        return self._request("ping", {})["d"]

    def status(self) -> dict:
        return self._request("status", {})["d"]

    def trigger_sync(self, kind: str, **params) -> dict:
        return self._request("trigger_sync", {"kind": kind, **params})["d"]

    def close(self) -> None:
        with self._sock_lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
