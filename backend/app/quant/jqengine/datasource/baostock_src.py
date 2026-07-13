"""baostock 5分钟数据源 -> 插值为1分钟。

mootdx 仅提供最近约92天1分钟线，baostock 提供5分钟线可覆盖更早历史。
每根5分钟K线拆成5根1分钟：OHLC线性插值，volume/amount均分。
"""
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
    """将5分钟K线插值为1分钟K线。

    每根5分钟bar -> 5根1分钟bar:
    - open: 第1根=open, 后4根=close方向线性插值
    - close: 线性插值 (5min_open -> 5min_close)
    - high/low: 5根都取5分钟的high/low
    - volume/amount: 均分5份
    """
    if df_5min is None or df_5min.empty:
        return pd.DataFrame()

    rows = []
    for _, bar in df_5min.iterrows():
        ts = bar.name
        o = float(bar["open"])
        c = float(bar["close"])
        h = float(bar["high"])
        low = float(bar["low"])
        v = float(bar.get("volume", 0))
        a = float(bar.get("amount", 0))

        for i in range(5):
            t = ts - pd.Timedelta(minutes=4 - i)
            frac = (i + 1) / 5.0
            close_i = o + (c - o) * frac
            open_i = o if i == 0 else o + (c - o) * (i / 5.0)
            rows.append({
                "datetime": t,
                "open": round(open_i, 4),
                "close": round(close_i, 4),
                "high": h,
                "low": low,
                "volume": v / 5.0,
                "amount": a / 5.0,
                "money": a / 5.0,
            })

    out = pd.DataFrame(rows)
    out.index = pd.to_datetime(out["datetime"])
    out.index.name = "datetime"
    out = out.drop(columns=["datetime"])
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

    def get_5min(self, code, start, end):
        """拉取5分钟K线，返回 DatetimeIndex DataFrame。"""
        self._ensure_login()
        import baostock as bs

        symbol = self._to_symbol(code)
        rs = bs.query_history_k_data_plus(
            symbol,
            "date,time,open,high,low,close,volume,amount",
            start_date=start,
            end_date=end,
            frequency="5",
            adjustflag="2",
        )
        if rs.error_code != "0":
            raise DataSourceError(f"baostock query failed: {rs.error_msg}")

        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            raise DataSourceError("baostock 无5分钟数据")

        df = pd.DataFrame(rows, columns=rs.fields)
        for col in ("open", "high", "low", "close", "volume", "amount"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.index = df["time"].apply(_parse_time)
        df.index.name = "datetime"
        return df.drop(columns=["date", "time"])

    def get_minute_1min(self, code, start, end):
        """拉取5分钟并插值为1分钟。"""
        df5 = self.get_5min(code, start, end)
        return interpolate_5min_to_1min(df5)

    # 以下接口 baostock 不作为主力源，仅满足基类
    def get_daily(self, code, start, end):
        raise DataSourceError("baostock daily 由 mootdx 覆盖")

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
