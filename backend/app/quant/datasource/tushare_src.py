"""Tushare 数据源实现。"""

import tushare as ts

from .base import DataSource, DataSourceError


def _to_ts_code(code):
    """平台代码(510300.XSHG / 000001.XSHE) -> Tushare ts_code(510300.SH / 000001.SZ)。"""
    if "." in code:
        sym, market = code.split(".", 1)
        market = market.upper()
        if market in ("XSHG", "SH", "SSE"):
            return f"{sym}.SH"
        if market in ("XSHE", "SZ", "SZSE"):
            return f"{sym}.SZ"
        return f"{sym}.{market[:2].upper()}"
    return code


class TushareSource(DataSource):
    name = "tushare"

    def __init__(self, token=""):
        self.token = token
        self._pro = None

    def _api(self):
        if not self.token:
            raise DataSourceError("未配置Tushare Token")
        if self._pro is None:
            ts.set_token(self.token)
            self._pro = ts.pro_api()
        return self._pro

    def get_daily(self, code, start, end):
        pro = self._api()
        df = pro.daily(ts_code=_to_ts_code(code),
                       start_date=str(start).replace("-", ""),
                       end_date=str(end).replace("-", ""))
        if df is None or df.empty:
            raise DataSourceError("Tushare 无日线数据")
        return df.sort_values("trade_date").reset_index(drop=True)

    def get_minute(self, code, date):
        pro = self._api()
        d = str(date).replace("-", "")
        df = ts.pro_bar(ts_code=_to_ts_code(code),
                        start_date=d + "09:00:00", end_date=d + "15:00:00",
                        freq="1min")
        if df is None or df.empty:
            raise DataSourceError("Tushare 无分钟数据")
        return df

    def get_index_realtime(self, codes):
        pro = self._api()
        out = []
        for c in codes:
            try:
                df = pro.index_daily(ts_code=_to_ts_code(c))
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    out.append({"code": c, "close": float(row["close"]),
                                "pct_chg": float(row.get("pct_chg", 0) or 0)})
            except Exception:
                continue
        if not out:
            raise DataSourceError("Tushare 指数无数据")
        return out

    def get_etf_list(self):
        pro = self._api()
        df = pro.fund_basic(market="E", status="L")
        if df is None or df.empty:
            raise DataSourceError("Tushare 无ETF池")
        lof_prefixes = ("160", "161", "162", "163", "164", "165",
                        "166", "167", "168", "501", "502", "508")
        etf = df[~df["ts_code"].str[:3].isin(lof_prefixes)]
        return etf[["ts_code", "name", "list_date", "delist_date"]].to_dict("records")

    def get_stock_list(self):
        pro = self._api()
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code")
        if df is None or df.empty:
            raise DataSourceError("Tushare 无股票池")
        return [r["ts_code"] for _, r in df.iterrows()]

    def get_us_index(self):
        pro = self._api()
        out = []
        for c in ["IXIC", "DJI", "SPX"]:
            try:
                df = pro.index_global(ts_code=c)
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    out.append({"code": c, "close": float(row["close"]),
                                "pct_chg": float(row.get("pct_chg", 0) or 0)})
            except Exception:
                continue
        if not out:
            raise DataSourceError("Tushare 美股指数无数据")
        return out

    def test_connection(self):
        try:
            self._api().trade_cal(exchange="SSE", limit=1)
            return True, "Tushare 连接正常"
        except DataSourceError as e:
            return False, str(e)
        except Exception as e:
            return False, str(e)
