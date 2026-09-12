"""method → handler 分发。每个 handler 返回 ("json"|"parquet", data)，
parquet 的 data 是 polars DataFrame，由 server 编码为 parquet 字节。"""
from __future__ import annotations

import datetime as _dt
import logging

from .sources import DataSources, _now, _to_jq, pd_to_ts

logger = logging.getLogger("app.services.stockdata.handlers")


def _norm_code(code: str) -> str:
    """客户端可能传 .XSHG/.XSHE/.SH/.SZ/裸 6 位，统一为 .XSHG/.XSHE。"""
    if "." in code:
        return _to_jq(code)
    # 裸 6 位：按数字前缀推断市场（5/6/9 沪市：5 开头为沪 ETF，9 开头为沪 B 股；
    # 其余 0/3/4/8 等深市），再归一
    return _to_jq(code + (".XSHG" if code[:1] in ("5", "6", "9") else ".XSHE"))


def _norm_codes(security) -> list[str]:
    if isinstance(security, str):
        return [_norm_code(security)]
    return [_norm_code(c) for c in security]


def h_ping(p, s: DataSources):
    return "json", {"pong": True, "ts": _dt.datetime.now().isoformat()}


def h_status(p, s: DataSources):
    # 返回 scheduler 状态（最近任务结果 + 当前正在执行的任务），供前端展示
    from .scheduler import get_status
    return "json", get_status()


def h_get_price(p, s: DataSources):
    codes = _norm_codes(p["security"])
    freq = p.get("frequency", "daily")
    start = p.get("start_date")
    end = p.get("end_date")
    fq = p.get("fq")
    if freq == "daily":
        df = s.get_daily(codes, start or "2000-01-01", end or _dt.date.today().isoformat())
        # fq="pre"/"qfq"：最新锚定前复权（聚宽 get_price fq 默认语义）。
        # 不传或传 none/raw 保持原始价（回测桥自有折算层，不受影响）。
        if fq in ("pre", "qfq"):
            df = s.apply_qfq_daily(df)
        return "parquet", df
    # 分钟：区间内（或当日）1m
    today = _now().date().isoformat()
    start, end = start or today, end or today
    lo_ts, hi_ts = pd_to_ts(start), pd_to_ts(end)
    # 旧版 DataManager 把日期序列化成午夜；兼容已运行客户端的整日语义，
    # 避免只重启 stockdata 后历史模拟盘整日无价。盘中上界仍原样保留。
    if hi_ts == hi_ts.normalize():
        hi_ts = hi_ts.replace(hour=15)
    df = s.get_minute(codes, str(lo_ts), str(hi_ts))
    return "parquet", df


def h_preload_daily(p, s: DataSources):
    lookback = int(p.get("lookback_days", 400))
    asof = p.get("asof")
    fq = p.get("fq")
    df = s.preload_daily(lookback, _dt.date.fromisoformat(asof) if asof else None)
    if fq in ("pre", "qfq"):
        df = s.apply_qfq_daily(df)
    return "parquet", df


def h_get_minute(p, s: DataSources):
    codes = _norm_codes(p["security"])
    df = s.get_minute(codes, p.get("lo_ts"), p.get("hi_ts"))
    return "parquet", df


def h_current_snapshot(p, s: DataSources):
    codes = _norm_codes(p["security"])
    df = s.get_realtime_snapshot(codes, p.get("as_of"))
    return "parquet", df


def h_get_trade_days(p, s: DataSources):
    return "json", s.get_trade_days(p.get("start_date", "2000-01-01"),
                                    p.get("end_date", _dt.date.today().isoformat()))


def h_get_all_securities(p, s: DataSources):
    types = p.get("types")
    df = s.get_all_securities(types, p.get("date"))
    return "parquet", df


def h_get_security_info(p, s: DataSources):
    return "json", s.get_security_info(_norm_code(p["code"]))


def h_get_security_infos(p, s: DataSources):
    return "json", s.get_security_infos(p.get("codes"))


def h_get_index_stocks(p, s: DataSources):
    return "json", s.get_index_stocks(_norm_code(p["index_code"]), p.get("date"))


def h_get_stock_names(p, s: DataSources):
    codes = p.get("codes")
    return "json", s.get_stock_names(codes)


def h_get_adj_factors(p, s: DataSources):
    return "parquet", s.get_adj_factors()


def h_get_financials(p, s: DataSources):
    return "parquet", s.get_financials()


def h_get_etf_nav(p, s: DataSources):
    codes = _norm_codes(p["security"])
    return "parquet", s.get_etf_nav(codes, p.get("date"))


def h_trigger_sync(p, s: DataSources):
    from .scheduler import trigger_sync
    kind = p.get("kind", "backfill")
    params = {k: v for k, v in p.items() if k != "kind"}
    trigger_sync(kind, **params)
    return "json", {"ok": True, "kind": kind}


HANDLERS = {
    "ping": h_ping,
    "status": h_status,
    "get_price": h_get_price,
    "preload_daily": h_preload_daily,
    "get_minute": h_get_minute,
    "current_snapshot": h_current_snapshot,
    "get_trade_days": h_get_trade_days,
    "get_all_securities": h_get_all_securities,
    "get_security_info": h_get_security_info,
    "get_security_infos": h_get_security_infos,
    "get_index_stocks": h_get_index_stocks,
    "get_stock_names": h_get_stock_names,
    "get_adj_factors": h_get_adj_factors,
    "get_financials": h_get_financials,
    "get_etf_nav": h_get_etf_nav,
    "trigger_sync": h_trigger_sync,
}


def handle(method: str, params: dict, src: DataSources):
    fn = HANDLERS.get(method)
    if fn is None:
        raise ValueError(f"未知 method: {method}")
    return fn(params, src)
