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


def round_half_up_price(x, decimals: int = 2):
    """价格四舍五入（交易所口径），替代 numpy 银行家舍入。

    交易所规则为四舍五入：9.95×1.1=10.945 → 10.95；np.round 半偶舍入得
    10.94，导致 601609 等股票的涨停判定整体错位（2026-09-30 首板高开策略
    对齐实测）。1e-6 容差吸收二进制浮点表示误差（10.945 的二进制近似为
    10.9449999…），对真实非半分位值（距 .005 远大于 1e-6）无影响。
    """
    import numpy as np

    scale = 10 ** decimals
    return np.floor(np.asarray(x, dtype="float64") * scale + 0.5 + 1e-6) / scale


def limit_prices_from_prev_close(close, rate: float = 0.10):
    """按昨收计算涨跌停价序列：limit = round_half_up(prev_close × (1±rate), 2)。"""
    prev_close = close.shift(1)
    limit_up = round_half_up_price(prev_close * (1 + rate))
    limit_down = round_half_up_price(prev_close * (1 - rate))
    return limit_up, limit_down
