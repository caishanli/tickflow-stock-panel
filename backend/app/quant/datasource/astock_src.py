"""a-stock-data 数据源实现。

a-stock-data 是 GitHub Skill (https://github.com/simonlin1212/a-stock-data)，
其能力已 vendor 到 :mod:`backend.app.datasource.astock_skill`。本类把平台
数据源接口映射到该 Skill 的函数；任何失败统一抛 :class:`DataSourceError`，
由上层降级，绝不造伪数据。
"""

import pandas as pd

from . import astock_skill as skill
from .base import DataSource, DataSourceError


def _to_symbol(code):
    """平台代码 -> 6位纯数字代码 (a-stock-data 函数要求)。"""
    return code.split(".")[0]


class AStockSource(DataSource):
    name = "astock"

    def __init__(self, token=""):
        pass

    def get_daily(self, code, start, end):
        sym = _to_symbol(code)
        start_time = str(start).replace("-", "")[:8]
        try:
            data = skill.baidu_kline_with_ma(sym, start_time)
        except Exception as e:
            raise DataSourceError(f"a-stock-data 日线失败: {e}")
        keys = data.get("keys", []) or []
        rows = [r for r in data.get("rows", []) if r and r.strip()]
        if not keys or not rows:
            raise DataSourceError("a-stock-data 无日线数据")
        try:
            records = [r.split(",") for r in rows]
            df = pd.DataFrame(records, columns=keys)
        except Exception as e:
            raise DataSourceError(f"a-stock-data 日线解析失败: {e}")
        return df

    def get_minute(self, code, date):
        sym = _to_symbol(code)
        try:
            client = skill.tdx_client()
            df = client.bars(symbol=sym, frequency=8)
        except Exception as e:
            raise DataSourceError(f"a-stock-data 分钟线失败: {e}")
        if df is None or df.empty:
            raise DataSourceError("a-stock-data 无分钟数据")
        return df

    def get_index_realtime(self, codes):
        syms = [_to_symbol(c) for c in codes]
        try:
            q = skill.tencent_quote(syms)
        except Exception as e:
            raise DataSourceError(f"a-stock-data 实时行情失败: {e}")
        out = []
        for c, s in zip(codes, syms):
            row = q.get(s)
            if row:
                out.append({"code": c, "close": float(row.get("price", 0)),
                            "pct_chg": float(row.get("change_pct", 0))})
        if not out:
            raise DataSourceError("a-stock-data 指数无数据")
        return out

    def get_etf_list(self):
        raise DataSourceError("a-stock-data 未提供全市场ETF池接口")

    def get_stock_list(self):
        raise DataSourceError("a-stock-data 未提供全市场股票池接口")

    def get_us_index(self):
        raise DataSourceError("a-stock-data 不支持美股指数")

    def test_connection(self):
        try:
            skill.tencent_quote(["000001"])
            return True, "a-stock-data 可用"
        except Exception as e:
            return False, str(e)
