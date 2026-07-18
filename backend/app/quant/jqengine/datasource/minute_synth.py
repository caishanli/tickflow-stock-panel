"""合成分钟线数据源。

当环境无法获取历史分钟 K 线（mootdx 仅返回最近约 800 根、腾讯/百度/Eastmoney
历史分钟接口在本环境不可用、Tushare stk_mins 需付费积分）时，把日线 OHLCV 展开为
每个交易日 240 根 1 分钟 bars，使分钟级回测引擎能够端到端运行并自恰。

价格为确定性插值（开盘->收盘线性 + 受 [low,high] 约束的正弦扰动），成交量按 U 形
分布切分。该数据用于验证引擎逻辑（日内任务调度、every_bar 止损、分钟级成交价），
并非真实行情；若后续接入真实历史分钟源，仅需替换 ``get_minute`` 实现即可。
"""

import numpy as np
import pandas as pd


# 交易时段：09:30-11:30（120 根）与 13:01-15:00（120 根），共 240 根
def _minute_index(day: pd.Timestamp) -> pd.DatetimeIndex:
    morning = [day.replace(hour=9, minute=30) + pd.Timedelta(minutes=i)
               for i in range(120)]
    afternoon = [day.replace(hour=13, minute=1) + pd.Timedelta(minutes=i)
                 for i in range(120)]
    return pd.DatetimeIndex(morning + afternoon)


def _expand_day(row: pd.Series) -> pd.DataFrame:
    o = float(row.get("open", 0) or 0)
    h = float(row.get("high", 0) or 0)
    lo = float(row.get("low", 0) or 0)
    c = float(row.get("close", 0) or 0)
    v = float(row.get("volume", 0) or 0)
    if o == 0 or c == 0:
        return pd.DataFrame()
    day = pd.Timestamp(row.name).normalize()
    idx = _minute_index(day)
    n = len(idx)
    frac = np.linspace(0.0, 1.0, n)
    # 线性开盘->收盘
    linear = o + (c - o) * frac
    # 受 [low,high] 约束的正弦扰动（确定性）
    wobble = (h - lo) * 0.25 * np.sin(2.0 * np.pi * frac * 3.0 + np.pi / 4)
    price = np.clip(linear + wobble, min(lo, h), max(lo, h))
    # U 形成交量分布（开盘/收盘放量，午间缩量）
    w = 1.0 + np.cos(2.0 * np.pi * frac)
    vols = v * w / w.sum() if w.sum() > 0 else np.full(n, v / n)
    return pd.DataFrame(
        {
            "open": price,
            "high": np.maximum.reduce([price, np.roll(price, -1)]),
            "low": np.minimum.reduce([price, np.roll(price, -1)]),
            "close": price,
            "volume": vols,
        },
        index=idx,
    )


class SyntheticMinuteSource:
    """由日线合成分钟线。``daily_getter(code, start, end) -> DataFrame`` 提供日线。"""

    name = "synthetic_minute"

    def __init__(self, daily_getter):
        self._daily = daily_getter
        self.window = None  # (start, end) 回测区间，由 manager 在回测前设定

    @staticmethod
    def _normalize_daily_index(daily):
        """把日线帧索引归一为 DatetimeIndex，无法归一返回 None。

        优先用日期列（datetime/trade_date/date/time）——tushare 日线帧是
        RangeIndex，若直接 ``pd.to_datetime(整数索引)`` 会被当成纳秒时间戳
        得到 1970 垃圾日期；仅当索引本身是 object（日期字符串等）时才直接
        转换。转换后丢弃 NaT 行。
        """
        if isinstance(daily.index, pd.DatetimeIndex):
            return daily
        daily = daily.copy()
        for col in ("datetime", "trade_date", "date", "time"):
            if col in daily.columns:
                idx = pd.to_datetime(daily[col], errors="coerce")
                if idx.notna().any():
                    daily.index = pd.DatetimeIndex(idx)
                    return daily[daily.index.notna()]
        if daily.index.dtype == object:
            idx = pd.to_datetime(daily.index, errors="coerce")
            if idx.notna().any():
                daily.index = pd.DatetimeIndex(idx)
                return daily[daily.index.notna()]
        return None

    def get_minute(self, code, end_date, start_date=None, count=None):
        end = pd.Timestamp(end_date) if end_date else pd.Timestamp.today().normalize()
        if start_date:
            # 桥接器时钟 feed：需要覆盖整个回测区间
            start = pd.Timestamp(start_date) - pd.Timedelta(days=10)
        elif self.window:
            start = pd.Timestamp(self.window[0]) - pd.Timedelta(days=10)
        else:
            # 策略内 get_price('1m') 仅需要近期分钟（当日量/近30分趋势），
            # 生成短窗口即可，避免每根 bar 返回数万行再截断
            start = end - pd.Timedelta(days=5)
        daily = self._daily(code, start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
        if daily is None or daily.empty:
            return pd.DataFrame()
        # 先归一为 DatetimeIndex 再切片：原实现先 ``daily.index >= lo`` 切片、
        # 后转换索引，tushare 日线帧（RangeIndex）直接抛 TypeError，合成兜底
        # 对非日期索引帧全部失效（报错“日线合成兜底失败”后退化为无数据跳过）。
        daily = self._normalize_daily_index(daily)
        if daily is None or daily.empty:
            return pd.DataFrame()
        # mootdx 忽略日期范围返回最近约800根日线，按窗口切片避免展开过多
        lo = start
        hi = end
        mask = (daily.index >= lo) & (daily.index <= hi)
        daily = daily[mask]
        if daily.empty:
            return pd.DataFrame()
        daily = daily.sort_index()
        frames = []
        for _t, row in daily.iterrows():
            f = _expand_day(row)
            if not f.empty:
                frames.append(f)
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames).sort_index()
        out = out[~out.index.duplicated(keep="last")]
        return out
