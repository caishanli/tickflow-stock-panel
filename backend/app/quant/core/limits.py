"""涨跌停推导——分档幅度与昨收公式的唯一实现。

真实数据层无 high_limit/low_limit/preclose 列，按昨收+分档幅度计算：
- 沪 68/58、深 30/159 → ±20%；ST → ±5%；其余 ±10%。
- limit = round(prev_close × (1±rate), 2)；首日无前收 → NaN（不估算）。
- 北交所 8/4 开头实为 ±30%，但本代码体系（XSHG/XSHE 后缀）覆盖不到北交所标的，
  不做分档，随主板 ±10%（旧 jqcompat 注释搬运，决策保留）。

调用方差异只在两处，各自保留薄适配：
- 码制：JQ（XSHG/XSHE）vs PTrade（SS/SZ）——调用方先归一化 ``exch`` 再调；
- ST 判定：名称源不同——调用方把 ``is_st`` 布尔传进来。
"""
from __future__ import annotations


def normalize_exchange(exch: str) -> str:
    """交易所后缀归一化：SS→XSHG、SZ→XSHE，其余原样（"" 透传，表包含它）。"""
    if exch == "SS":
        return "XSHG"
    if exch == "SZ":
        return "XSHE"
    return exch or ""


def limit_rate(pure: str, exch: str, is_st: bool = False) -> float:
    """按纯代码+归一化交易所+ST 标志返回涨跌停幅度。"""
    exch = normalize_exchange(exch)
    if exch in ("", "XSHG") and pure.startswith(("68", "58")):
        return 0.20
    if exch in ("", "XSHE") and pure.startswith(("30", "159")):
        return 0.20
    if is_st:
        return 0.05
    return 0.10


def limit_prices_from_prev_close(close, rate: float = 0.10):
    """按昨收计算涨跌停价序列：limit = round(prev_close × (1±rate), 2)。"""
    prev_close = close.shift(1)
    limit_up = (prev_close * (1 + rate)).round(2)
    limit_down = (prev_close * (1 - rate)).round(2)
    return limit_up.to_numpy(dtype="float64"), limit_down.to_numpy(dtype="float64")
