"""模拟盘进程内标的名称解析。

名称来源：stockdata 服务 get_stock_names（客户端 StockDataClient 透传），
返回 {纯6位代码: 名称}；本模块转成 {JQ码: 名称} 并在进程内缓存。
任何失败降级为空映射 → resolve_name 回退代码，不影响行情正确性。
"""
from __future__ import annotations

import logging

log = logging.getLogger("app.quant.simulate.names")

_NAMES: dict[str, str] | None = None  # {JQ码: 名称}


def _to_jq(pure: str, symbol: str) -> str:
    """纯代码 + 分区符号(.SH/.SZ) -> JQ码(.XSHG/.XSHE)。"""
    suffix = symbol.rsplit(".", 1)[-1]
    return pure + (".XSHG" if suffix in ("SH", "XSHG") else ".XSHE")


def get_name_map() -> dict[str, str]:
    """返回 {JQ码: 名称}，进程内缓存。失败返回空映射。"""
    global _NAMES
    if _NAMES is not None:
        return _NAMES
    out: dict[str, str] = {}
    try:
        from ..datasource.network_client import StockDataClient
        client = StockDataClient()
        # 服务端返回 {纯6位代码: 名称}，无分区符号 → 无法直接转 JQ 后缀。
        # 因此客户端映射键直接保留纯代码，resolve_name 按纯代码查。
        raw = client.get_stock_names() or {}
        for pure, name in raw.items():
            if name:
                out[pure] = str(name)
    except Exception:
        log.warning("get_stock_names 失败，标的名称回退代码", exc_info=True)
    _NAMES = out
    return out


def resolve_name(code: str) -> str:
    """按标的代码（JQ 码或纯代码）查名称，缺失回退代码本身。"""
    pure = code.split(".", 1)[0]
    return get_name_map().get(pure) or code
