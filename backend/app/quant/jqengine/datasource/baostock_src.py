"""baostock 5分钟数据源 -> 插值为1分钟。

mootdx 仅提供最近约92天1分钟线，baostock 提供5分钟线可覆盖更早历史。
每根5分钟K线拆成5根1分钟：OHLC线性插值，volume/amount均分。
"""
import threading

import pandas as pd

from .base import DataSource, DataSourceError


def _to_baostock_code(code):
    """平台代码 -> baostock代码 (sh.510300 / sz.159915)。"""
    pure = code.split(".")[0]
    if pure.startswith(("5", "6", "9")):
        return f"sh.{pure}"
    return f"sz.{pure}"


def _parse_time(time_str):
    """baostock time格式 '20260105093500000' -> Timestamp。"""
    s = str(time_str)
    return pd.Timestamp(f"{s[:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}:{s[10:12]}:{s[12:14]}")


def interpolate_5min_to_1min(df_5min):
    """将5分钟K线插值为1分钟K线（向量化实现）。

    每根5分钟bar -> 5根1分钟bar:
    - open: 第1根=open, 后4根=close方向线性插值
    - close: 线性插值 (5min_open -> 5min_close)
    - high/low: 5根都取5分钟的high/low
    - volume/amount: 均分5份
    """
    if df_5min is None or df_5min.empty:
        return pd.DataFrame()

    import numpy as np
    n = len(df_5min)
    o = df_5min["open"].to_numpy(dtype=np.float64)
    c = df_5min["close"].to_numpy(dtype=np.float64)
    h = df_5min["high"].to_numpy(dtype=np.float64)
    low = df_5min["low"].to_numpy(dtype=np.float64)
    v = df_5min["volume"].to_numpy(dtype=np.float64) if "volume" in df_5min.columns else np.zeros(n)
    a = df_5min["amount"].to_numpy(dtype=np.float64) if "amount" in df_5min.columns else np.zeros(n)

    ts = df_5min.index.to_numpy().astype("datetime64[s]")
    offsets = np.array([-4, -3, -2, -1, 0], dtype="timedelta64[m]")   # (5,)
    t_1m = (ts[:, None] + offsets).ravel()                            # (n*5,) 升序

    frac_c = np.array([1, 2, 3, 4, 5], dtype=np.float64) / 5.0       # close 插值系数
    frac_o = np.array([0, 1, 2, 3, 4], dtype=np.float64) / 5.0       # open 插值系数
    spread = (c - o)[:, None]
    close_1m = (o[:, None] + spread * frac_c).ravel()
    open_1m = (o[:, None] + spread * frac_o).ravel()

    out = pd.DataFrame({
        "open": np.round(open_1m, 4),
        "close": np.round(close_1m, 4),
        "high": np.repeat(h, 5),
        "low": np.repeat(low, 5),
        "volume": np.repeat(v / 5.0, 5),
        "amount": np.repeat(a / 5.0, 5),
        "money": np.repeat(a / 5.0, 5),
    }, index=pd.DatetimeIndex(t_1m, name="datetime"))
    return out.sort_index()


class BaostockSource(DataSource):
    name = "baostock"

    def __init__(self):
        self._logged_in = False

    def _ensure_login(self):
        if self._logged_in:
            return
        import baostock as bs
        lg = bs.login()
        if lg.error_code != "0":
            raise DataSourceError(f"baostock login failed: {lg.error_msg}")
        self._logged_in = True

    def _to_symbol(self, code):
        return _to_baostock_code(code)

    def get_5min(self, code, start, end, timeout=240):
        """拉取5分钟K线，返回 DatetimeIndex DataFrame。

        注意: baostock 的 ``query_history_k_data_plus`` 在请求提交后，
        ``rs.next()`` 逐行取数阶段可能因服务端不回包而**永久阻塞**。这里把
        整个取数过程放进守护线程并加超时，超时即抛 ``DataSourceError``，
        由上层降级到缓存/下一数据源，避免回测整体卡死。
        """
        self._ensure_login()
        import baostock as bs

        symbol = self._to_symbol(code)
        _res = {}

        def _worker():
            try:
                rs = bs.query_history_k_data_plus(
                    symbol,
                    "date,time,open,high,low,close,volume,amount",
                    start_date=start,
                    end_date=end,
                    frequency="5",
                    adjustflag="2",
                )
                _res["err"] = rs.error_code
                _res["fields"] = list(rs.fields)
                rows = []
                while rs.next():
                    rows.append(rs.get_row_data())
                _res["rows"] = rows
            except Exception as e:  # noqa: BLE001
                _res["err"] = f"exc:{e}"

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            raise DataSourceError(
                f"baostock 取数超时({timeout}s): {code} {start}~{end}"
            )
        if str(_res.get("err", "unknown")).startswith("exc:"):
            raise DataSourceError(f"baostock 取数异常: {_res['err']}")
        if _res.get("err") != "0":
            raise DataSourceError(f"baostock query failed: {_res.get('err')}")

        rows = _res.get("rows") or []
        if not rows:
            raise DataSourceError("baostock 无5分钟数据")

        df = pd.DataFrame(rows, columns=_res.get("fields") or ["date","time","open","high","low","close","volume","amount"])
        for col in ("open", "high", "low", "close", "volume", "amount"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.index = df["time"].apply(_parse_time)
        df.index.name = "datetime"
        return df.drop(columns=["date", "time"])

    def get_minute_1min(self, code, start, end):
        """拉取5分钟并插值为1分钟。"""
        df5 = self.get_5min(code, start, end)
        return interpolate_5min_to_1min(df5)

    # baostock 前复权日线：因子较全，对齐聚宽前复权价（tushare adj 对部分
    # 新股 ETF 复权因子缺失，会返回未复权价导致除权跳变）。
    def get_daily(self, code, start, end, timeout=120):
        self._ensure_login()
        import baostock as bs

        symbol = self._to_symbol(code)
        # baostock 需要 YYYY-MM-DD 格式
        def _norm(d):
            d = str(d).replace("-", "").strip()
            return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else str(d)
        s = _norm(start)
        e = _norm(end)
        _res = {}

        def _worker():
            try:
                rs = bs.query_history_k_data_plus(
                    symbol,
                    "date,open,high,low,close,volume,amount",
                    start_date=s,
                    end_date=e,
                    frequency="d",
                    adjustflag="1",  # 前复权
                )
                _res["err"] = rs.error_code
                _res["fields"] = list(rs.fields)
                rows = []
                while rs.next():
                    rows.append(rs.get_row_data())
                _res["rows"] = rows
            except Exception as ex:  # noqa: BLE001
                _res["err"] = f"exc:{ex}"

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            raise DataSourceError(
                f"baostock 日线取数超时({timeout}s): {code} {start}~{end}"
            )
        if str(_res.get("err", "unknown")).startswith("exc:"):
            raise DataSourceError(f"baostock 日线取数异常: {_res['err']}")
        if _res.get("err") != "0":
            raise DataSourceError(f"baostock 日线 query failed: {_res.get('err')}")

        rows = _res.get("rows") or []
        if not rows:
            raise DataSourceError("baostock 无日线数据")

        cols = _res.get("fields") or [
            "date", "open", "high", "low", "close", "volume", "amount"]
        df = pd.DataFrame(rows, columns=cols)
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        # 列名对齐 tushare 源（trade_date 等），供 cache 统一存储
        df = df.rename(columns={"date": "trade_date"})
        df["trade_date"] = df["trade_date"].astype(str)
        df = df.sort_values("trade_date").reset_index(drop=True)
        return df

    def get_minute(self, code, date):
        return self.get_minute_1min(code, date, date)

    def get_index_realtime(self, codes):
        raise DataSourceError("baostock 不支持实时")

    def get_etf_list(self):
        raise DataSourceError("baostock 未配置ETF池")

    def get_stock_list(self):
        raise DataSourceError("baostock 未配置股票池")

    def get_us_index(self):
        raise DataSourceError("baostock 不支持美股")

    def test_connection(self):
        try:
            self._ensure_login()
            return True, "baostock 可用"
        except Exception as e:
            return False, str(e)
