"""mootdx (通达信) 数据源实现。"""

import pandas as pd

from .base import DataSource, DataSourceError


def _to_symbol(code):
    """平台代码 -> mootdx 6位纯数字代码。"""
    return code.split(".")[0]


def _is_index(code):
    """判断是否为指数代码（000xxx.SH / 399xxx.SZ）。"""
    pure = code.split(".")[0]
    return pure.startswith("399") or (pure.startswith("000") and len(pure) == 6
                                       and not pure.startswith("0000"))


# 证券名称缓存（内存级，跨回测复用；另落盘 data/.stock_names_cache.json）
_STOCK_NAMES_CACHE = None


class MootdxSource(DataSource):
    name = "mootdx"

    def __init__(self, token=""):
        self._client = None

    def _api(self):
        if self._client is None:
            try:
                from mootdx.quotes import Quotes
                self._client = Quotes.factory(market="std")
            except Exception as e:
                raise DataSourceError(f"mootdx 初始化失败: {e}")
        return self._client

    def get_daily(self, code, start, end):
        c = self._api()
        sym = _to_symbol(code)
        if _is_index(code):
            df = c.index_bars(symbol=sym, frequency=9)
        else:
            df = c.bars(symbol=sym, frequency=9)
        if df is None or df.empty:
            raise DataSourceError("mootdx 无日线数据")
        # 兼容 JQ 字段名：成交额 = amount -> money；成交量 = vol -> volume
        if "amount" in df.columns:
            df["money"] = df["amount"]
        if "vol" in df.columns and "volume" not in df.columns:
            df["volume"] = df["vol"]
        if "volume" in df.columns:
            df["volume"] = df["volume"] * 100
        return df

    def get_minute(self, code, date=""):
        """历史 1 分钟 K 线：mootdx 单次最多约 800 根，按 ``start`` 分页回看。"""
        c = self._api()
        sym = _to_symbol(code)
        frames = []
        start = 0
        offset = 800
        for _ in range(400):
            df = c.bars(symbol=sym, frequency=8, start=start, offset=offset)
            if df is None or df.empty:
                break
            frames.append(df)
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
