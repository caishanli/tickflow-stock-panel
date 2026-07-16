"""mootdx (通达信) 数据源实现。"""

import socket
import pandas as pd

from mootdx.quotes import Quotes
from .base import DataSource, DataSourceError


def _to_symbol(code):
    """平台代码 -> mootdx 6位纯数字代码。"""
    return code.split(".")[0]


def _is_index(code):
    """判断是否为指数代码（000xxx.SH / 399xxx.SZ）。"""
    pure = code.split(".")[0]
    return pure.startswith("399") or (pure.startswith("000") and len(pure) == 6) \
        and not pure.startswith("0000")


# 显式 mootdx 行情服务器列表（TCP 7709）。顺序探测，用第一个可达的，
# 规避 0.11.x BESTIP.HQ 空串 bug；海外网络通常全部超时，此时回退
# 到 mootdx 自带 bestip 测速 / 裸 factory。
_TDX_SERVERS = [
    ('119.97.185.59', 7709), ('124.70.133.119', 7709), ('116.205.183.150', 7709),
    ('123.60.73.44', 7709), ('116.205.163.254', 7709), ('121.36.225.169', 7709),
    ('123.60.70.228', 7709), ('124.71.9.153', 7709), ('110.41.147.114', 7709),
    ('124.71.187.122', 7709),
]


def _probe(ip, port, timeout=2.0):
    """TCP 握手探测，判断服务器是否可达。"""
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def tdx_client(market='std'):
    """
    创建 mootdx 客户端，规避 0.11.x BESTIP.HQ 空串 bug。
    顺序兜底，保证 IP 列表老化/换网时仍能工作：
      1) 顺序探测 _TDX_SERVERS，用第一个 TCP 可达的显式 server
         （不做数据探活：某服务器对 510300 无数据不代表对其他标的
         也无数据，数据为空由上层 _with_server_retry 换服务器兜底）；
      2) 显式列表全不可达 → 回退 mootdx 自带 bestip 测速选优；
      3) 再不行 → 回退裸 factory（老用户 config 已有可用 BESTIP 时成立）；
      4) 仍失败 → 抛 RuntimeError，明确报错而非死等。
    """
    for ip, port in _TDX_SERVERS:
        if _probe(ip, port):
            try:
                return Quotes.factory(market=market, server=(ip, port))
            except Exception:
                continue
    try:
        return Quotes.factory(market=market)                # fallback 1（裸 factory 实测可用）
    except Exception:
        pass
    try:
        return Quotes.factory(market=market, bestip=True)   # fallback 2
    except Exception as e:
        raise RuntimeError(
            "所有 mootdx 服务器均不可达。海外网络通常全部超时（TCP 7709），"
            "请走国内代理或更新 _TDX_SERVERS 列表。原始错误：%s" % e
        )


# 证券名称缓存（内存级，跨回测复用；另落盘 data/.stock_names_cache.json）
_STOCK_NAMES_CACHE = None


class MootdxSource(DataSource):
    name = "mootdx"

    def __init__(self, token=""):
        self._client = None
        self._server_idx = -1  # 当前使用的 _TDX_SERVERS 下标（-1=尚未显式选定）

    def _api(self):
        if self._client is None:
            try:
                from mootdx.quotes import Quotes
                # 裸 factory（自动选速）实测对全市场标的都能取到数据且快，
                # 作为默认客户端；显式 _TDX_SERVERS 仅作失败时的轮换兜底。
                self._client = Quotes.factory(market="std")
            except Exception as e:
                raise DataSourceError(f"mootdx 初始化失败: {e}")
        return self._client

    def _rotate_server(self, to_bestip=False):
        """换服务器重建客户端（运行时取数超时/失败兜底）。

        - to_bestip=False：换到 _TDX_SERVERS 下一个 TCP 可达的显式服务器；
        - to_bestip=True：回退 mootdx 自带 bestip（实测裸 factory 已能
          工作，bestip 仅作最后兜底）。
        返回新客户端；全部失败返回 None。
        """
        from mootdx.quotes import Quotes
        if to_bestip:
            try:
                self._client = Quotes.factory(market="std", bestip=True)
                return self._client
            except Exception:
                return None
        n = len(_TDX_SERVERS)
        for step in range(1, n + 1):
            idx = (self._server_idx + step) % n
            ip, port = _TDX_SERVERS[idx]
            if not _probe(ip, port):
                continue
            try:
                client = Quotes.factory(market="std", server=(ip, port))
                self._server_idx = idx
                self._client = client
                return client
            except Exception:
                continue
        # 显式列表用尽 → 回退 bestip
        return self._rotate_server(to_bestip=True)

    def _with_server_retry(self, fn, empty_ok=False):
        """执行取数 ``fn``，超时/返回空时按 _TDX_SERVERS 轮询换服务器重试。

        - fn 内阻塞超过 15s 视为超时（线程守护，超时即换服务器）；
        - 返回 None/空 且 ``empty_ok=False`` 也触发换服务器；
        - 列表用尽后回退到 bestip/factory 兜底客户端（_api 重建）。
        返回 (df, err)：成功 df 非 None，失败二者皆 None/err 说明。
        """
        import threading
        attempts = len(_TDX_SERVERS) + 1  # 显式列表各一次 + 末次 bestip/factory 兜底
        for attempt in range(attempts):
            c = self._api()
            box = {}
            def _run():
                try:
                    box["df"] = fn(c)
                except Exception as e:
                    box["err"] = e
            t = threading.Thread(target=_run, daemon=True)
            t.start()
            t.join(10)
            if t.is_alive():
                self._rotate_server(to_bestip=(attempt == attempts - 1))
                continue
            if "err" in box:
                self._rotate_server(to_bestip=(attempt == attempts - 1))
                continue
            df = box.get("df")
            if df is None or (not empty_ok and (hasattr(df, "empty") and df.empty)):
                self._rotate_server(to_bestip=(attempt == attempts - 1))
                continue
            return df, None
        return None, "mootdx 所有服务器均超时/无数据"

    def get_daily(self, code, start, end):
        sym = _to_symbol(code)
        def _fn(c):
            if _is_index(code):
                return c.index_bars(symbol=sym, frequency=9)
            return c.bars(symbol=sym, frequency=9)
        df, err = self._with_server_retry(_fn)
        if df is None or df.empty:
            raise DataSourceError(f"mootdx 无日线数据 ({err})")
        # 兼容 JQ 字段名：成交额 = amount -> money；成交量 = vol -> volume
        if "amount" in df.columns:
            df["money"] = df["amount"]
        if "vol" in df.columns and "volume" not in df.columns:
            df["volume"] = df["vol"]
        if "volume" in df.columns:
            df["volume"] = df["volume"] * 100
        return df

    def get_minute(self, code, date="", max_bars=30000):
        """历史 1 分钟 K 线：mootdx 单次最多约 800 根，按 ``start`` 分页回看。

        ``max_bars`` 上限防止对"无数据/长期停牌"标的空转 400 页（每页一次
        阻塞式 socket 调用，单只可卡数分钟）；达到上限即停止分页。
        取数超时/返回空时自动按 _TDX_SERVERS 轮询换服务器重试。
        """
        sym = _to_symbol(code)
        box = {}
        def _fn(c):
            # 先试拉 1 页判断是否有数据：无数据立即失败，避免对停牌标的
            # 空转 400 页。
            first = c.bars(symbol=sym, frequency=8, start=0, offset=800)
            if first is None or first.empty:
                raise DataSourceError("mootdx 无分钟数据")
            box["c"] = c
            return first
        first, err = self._with_server_retry(_fn)
        if first is None:
            raise DataSourceError(f"mootdx 无分钟数据 ({err})")
        c = box.get("c")
        frames = [first]
        fetched = len(first)
        start = 800
        offset = 800
        for _ in range(399):
            if fetched >= max_bars:
                break
            try:
                df = c.bars(symbol=sym, frequency=8, start=start, offset=offset)
            except Exception:
                break
            if df is None or df.empty:
                break
            frames.append(df)
            fetched += len(df)
            if len(df) < offset:
                break
            start += offset
        if not frames:
            raise DataSourceError("mootdx 无分钟数据")
        out = pd.concat(frames)
        out = out[~out.index.duplicated(keep="last")].sort_index()
        if "vol" in out.columns and "volume" not in out.columns:
            out["volume"] = out["vol"]
        if "amount" in out.columns and "money" not in out.columns:
            out["money"] = out["amount"]
        out.index.name = "datetime"
        if "datetime" in out.columns:
            out = out.drop(columns=["datetime"])
        return out

    def get_index_realtime(self, codes):
        raise DataSourceError("mootdx 暂不支持指数实时")

    def get_etf_list(self):
        raise DataSourceError("mootdx 未配置ETF池，请优先使用Tushare")

    def get_stock_names(self):
        """返回 {code: name} 字典，名称来自通达信行情（与聚宽 display_name 一致）。

        证券名称在回测期间不变，且仅供展示。每次回测都通过 TDX 网络拉全量
        名录代价极高（单次回测 ~15s），故在内存 + 本地文件做缓存：进程内只取
        一次，跨进程/跨回测直接命中本地缓存，避免重复网络往返。不影响行情正确性。
        """
        import json
        import os

        global _STOCK_NAMES_CACHE
        if _STOCK_NAMES_CACHE is not None:
            return _STOCK_NAMES_CACHE
        cache_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            "data", ".stock_names_cache.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    _STOCK_NAMES_CACHE = json.load(f)
                return _STOCK_NAMES_CACHE
            except Exception:
                pass
        c = self._api()
        out = {}
        for market in (0, 1):
            try:
                df = c.stocks(market=market)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    code = str(row["code"]).strip()
                    name = str(row["name"]).replace("\x00", "").strip()
                    out[code] = name
            except Exception:
                continue
        _STOCK_NAMES_CACHE = out
        try:
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False)
        except Exception:
            pass
        return out

    def get_stock_list(self):
        raise DataSourceError("mootdx 未配置股票池，请优先使用Tushare")

    def get_us_index(self):
        raise DataSourceError("mootdx 不支持美股")

    def test_connection(self):
        try:
            self._api()
            return True, "mootdx 连接正常"
        except Exception as e:
            return False, str(e)
