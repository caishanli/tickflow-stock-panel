"""数据单位契约：把「这个数是什么单位」写死在代码里，而不是散落在各解析点。

已知的坑（2026-11 复盘）：

- 腾讯 ``mkline``（m1）对主板/创业板/ETF 返回的成交量是**手**，但对科创板
  688/689 直接返回**股**。旧解析器一律 ×100，导致 09-10 起 ``kline_minute``
  里科创板分钟量（以及由量估出的 amount）全部 100 倍虚高；
- 日线落盘单位也不统一：``kline_daily`` 存**手**，``kline_etf_daily`` /
  ``kline_index_daily`` 存**股**（读侧不换算）。

用法：生产端用 :func:`tencent_m1_volume_factor` 做源单位 → 股；校验端用
:func:`minute_volume_to_daily` 做分钟（股）→ 日线口径。
"""

from __future__ import annotations

#: 科创板代码前缀（上交所 688 主板科创板 / 689 CDR）。
STAR_PREFIXES = ("688", "689")

#: 日线表 → volume 落盘单位（``"lot"`` 手 / ``"share"`` 股）。
DAILY_VOLUME_UNIT: dict[str, str] = {
    "kline_daily": "lot",
    "kline_etf_daily": "share",
    "kline_index_daily": "share",
}

#: 分钟表 → 对应日线表（分钟量统一存「股」，日线口径见 ``DAILY_VOLUME_UNIT``）。
MINUTE_TABLE_TO_DAILY: dict[str, str] = {
    "kline_minute": "kline_daily",
    "kline_etf_minute": "kline_etf_daily",
}


def tencent_m1_volume_factor(symbol: str) -> float:
    """腾讯 mkline 返回量单位：主板/创业板/ETF 给「手」，科创板 688/689 给「股」。

    ``symbol`` 形如 ``688001.SH``；返回源值 ×factor = 股。
    """
    code = symbol.split(".")[0]
    return 1.0 if code.startswith(STAR_PREFIXES) else 100.0


def minute_volume_to_daily(volume_shares: float, table: str) -> float:
    """把分钟成交量（股）折算成目标日线表的口径。

    ``table`` 可传分钟表名（``kline_minute`` / ``kline_etf_minute``）或日线表名
    （``kline_daily`` / ``kline_etf_daily``）。股票口径日线（手）→ ÷100；ETF /
    指数（股）→ 原样返回。未知表名一律原样返回（不猜）。
    """
    daily_table = MINUTE_TABLE_TO_DAILY.get(table, table)
    if DAILY_VOLUME_UNIT.get(daily_table) == "lot":
        return volume_shares / 100.0
    return volume_shares
