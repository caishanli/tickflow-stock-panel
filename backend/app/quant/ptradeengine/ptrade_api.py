"""PTrade 兼容 API 子集（本地引擎版，镜像 jqengine/engine/jq/api.py）。

在 ptrade 策略命名空间中注入本模块函数，使 PTrade 风格策略（get_history /
order / get_position / run_daily(context, func, time) 等）可在本地单机引擎
（模拟盘 runner / 离线回测）直接运行。

代码域：**PTrade**（.SS/.SZ）。策略面向的 API（get_history 列、get_positions
键、order 入参、快照）与引擎内部（ctx.portfolio / trades / minute_prices /
no_buy / no_sell）统一 PTrade 码；仅在与 DataManager（JQ 码 .XSHG/.XSHE）
交界处转换。``_state`` 形状与 jq_api._state 一致，runner plumbing 直接兼容。

撮合口径（与 jq api.order 相同）：100 股整手、T+1、佣金双边 + 卖出印花税
（非 ETF）、no_buy/no_sell 禁买卖。
"""
from __future__ import annotations

import types
from typing import ClassVar

import pandas as pd

from .context import PtradeContext, PtradePortfolio, PtradePosition, ptrade_code_conv

DEFAULT_STAMP_TAX = 0.0005  # 卖出印花税（A股 0.05%，ETF 免征）

# 官方 get_history 输出字段（用于单标的列名判定）
_PT_FIELDS = ("open", "high", "low", "close", "volume", "money", "price",
              "is_open", "preclose", "high_limit", "low_limit", "unlimited")

_state = {
    "ctx": None,
    "manager": None,
    "fee": 0.0003,
    "slippage": 0.001,
    "fee_config": None,
    "daily": [],
    "minute": [],
    "records": [],
    "trades": [],
    "minute_prices": {},
    "minute_mode": False,
    "no_buy": set(),
    "no_sell": set(),
    "log_sink": None,
}

_conv = ptrade_code_conv()
to_engine, to_pt = _conv


def _is_etf(code):
    """简单代码前缀判定：沪市 5 开头、深市 15/16 开头为基金（免印花税）。"""
    num = (code or "").split(".")[0]
    return num.startswith(("5", "15", "16"))


def _norm_freq(freq):
    freq = (freq or "1d").lower()
    if freq in ("daily", "day", "1d"):
        return "1d"
    if freq in ("min", "minute", "1m"):
        return "1m"
    return freq


def _reset(manager, fee, slippage, cash):
    ctx = PtradeContext()
    ctx.portfolio = PtradePortfolio(cash)
    _state.update(ctx=ctx, manager=manager, fee=fee, slippage=slippage,
                  fee_config=None, daily=[], minute=[], records=[], trades=[],
                  minute_prices={}, minute_mode=False,
                  no_buy=set(), no_sell=set(), log_sink=None)
    return ctx


def on_new_day():
    """新交易日钩子：清零 T+1 当日买入冻结量。"""
    ctx = _state.get("ctx")
    if ctx is not None and ctx.portfolio is not None:
        for pos in ctx.portfolio.positions.values():
            pos.today_amount = 0.0


def run_daily(context, func, time="HH:MM"):
    """PTrade run_daily：注册 (func, time)，由 runner 按到点触发。"""
    _state["daily"].append((func, str(time)))


def run_minute(func, minute="every"):
    _state["minute"].append((func, str(minute)))


def _live_price(security):
    """当前价：优先 minute_prices 快照（PTrade 域），其次 manager 精确取点。"""
    snap = _state.get("minute_prices") or {}
    if snap.get(security):
        return snap[security]
    if _state.get("minute_mode"):
        mgr = _state.get("manager")
        ctx = _state.get("ctx")
        if mgr and ctx and ctx.current_dt is not None:
            p = mgr.get_minute_price_at(to_engine(security), ctx.current_dt)
            if p is not None:
                return p
    return _state["ctx"].portfolio.get_position(security).price or 0


def order(security, amount):
    """按股数下单（正买负卖）。PTrade 域：security 直接用 .SS/.SZ。"""
    ctx = _state["ctx"]
    p = ctx.portfolio
    price = _live_price(security)
    if price == 0 or amount == 0:
        return False
    amount = int(amount)
    if amount > 0:
        amount = amount // 100 * 100  # A股/ETF 买入整手（100 股向下取整）
        if amount <= 0 or security in (_state.get("no_buy") or ()):
            return False
    existing = p.positions.get(security)
    prev_cost = float(existing.avg_cost or 0.0) if existing else 0.0
    if amount < 0:
        if security in (_state.get("no_sell") or ()):
            return False
        closeable = float(existing.closeable_amount) if existing else 0.0
        amount = -min(-amount, closeable)
        if amount == 0:
            return False
    fee = _state["fee"]
    slip = _state["slippage"]
    fill = price * (1 + slip) if amount > 0 else price * (1 - slip)
    fill = round(fill, 3)
    turnover = abs(amount) * fill
    fee_cfg = _state.get("fee_config")
    if fee_cfg:
        comm_rate = fee_cfg["open_commission"] if amount > 0 else fee_cfg["close_commission"]
        min_comm = fee_cfg["min_commission"]
    else:
        comm_rate = fee
        min_comm = 0.0
    fee_amount = round(max(turnover * comm_rate, min_comm), 2)
    tax_amount = 0.0
    if amount > 0:
        cost = turnover + fee_amount
        if cost > p.cash:
            return False
    else:
        tax_amount = 0.0 if _is_etf(security) else round(turnover * DEFAULT_STAMP_TAX, 2)
        cost = -(turnover - fee_amount - tax_amount)
    pos = p.positions.setdefault(security, PtradePosition())
    if amount > 0:
        if float(pos.amount or 0.0) <= 0:
            pos.entry_ts = ctx.current_dt
        total_cost = pos.amount * pos.avg_cost + amount * fill
        pos.amount += amount
        pos.avg_cost = total_cost / pos.amount if pos.amount else 0.0
        pos.today_amount = float(pos.today_amount or 0.0) + amount
    else:
        pos.amount += amount
        if pos.amount <= 0:
            pos.amount = 0
            pos.avg_cost = 0.0
            p.positions.pop(security, None)
    pos.price = price
    p.cash -= cost
    _state["trades"].append({
        "dt": ctx.current_dt, "code": security, "side": "buy" if amount > 0 else "sell",
        "amount": amount, "price": fill, "fee": fee_amount, "tax": tax_amount,
        "avg_cost": prev_cost,
    })
    return True


def order_value(security, value):
    price = _live_price(security)
    if price == 0 or value == 0:
        return False
    amount = int(value // (price * (1 + _state["fee"])))
    return order(security, amount)


def order_target_percent(security, percent):
    p = _state["ctx"].portfolio
    price = _live_price(security)
    if price == 0:
        return False
    target_value = p.value * percent
    target_shares = int(target_value // price) if percent > 0 else 0
    current_amount = p.get_position(security).amount
    amount = target_shares - current_amount
    if amount != 0:
        return order(security, amount)
    return True


def get_position(security):
    pos = _state["ctx"].portfolio.positions.get(security)
    if pos is None:
        return PtradePosition()
    return pos


def get_positions():
    return {c: p for c, p in _state["ctx"].portfolio.positions.items() if p.amount > 0}


def set_universe(codes):
    if isinstance(codes, str):
        codes = [codes]
    _state["ctx"].universe = list(codes)  # PTrade 域（runner feed 前转引擎码）


def get_history(count, frequency, field, security_list=None, include=True, fq="pre", end_dt=None):
    """PTrade get_history：多标的宽表（index=datetime, columns=PTrade 码）。

    数据走 DataManager（JQ 码），fq='pre' 与 jq get_price 同口径。end_dt 覆盖
    当前上下文时间（get_price 的 end_date 透传）。"""
    mgr = _state.get("manager")
    ctx = _state.get("ctx")
    if mgr is None:
        return pd.DataFrame()
    if security_list is None:
        codes = list(getattr(ctx, "universe", []) or [])
    elif isinstance(security_list, str):
        codes = [security_list]
    else:
        codes = list(security_list)
    if not codes:
        return pd.DataFrame()
    freq = _norm_freq(frequency)
    engine_codes = [to_engine(c) for c in codes]
    # 本地 DataManager 日线列为 money（原始成交额，与 jq get_price 口径一致），
    # 非 rqalpha recarray 的 total_turnover。
    col = "money" if field == "money" else field
    now = pd.Timestamp(end_dt) if end_dt is not None else (
        pd.Timestamp(ctx.current_dt) if (ctx and ctx.current_dt is not None) else None)
    # money_corrected：修正后的元成交额（对齐聚宽 get_daily_money_cached 口径，
    # 本地日线 money 列单位经 _ensure_money_yuan 修正，history_bars 的
    # total_turnover=close×volume 在部分 ETF 上被放大）
    if field == "money_corrected":
        try:
            df = mgr.get_daily_money_cached(engine_codes,
                                            str(now.date()) if now is not None else None,
                                            int(count))
            if df is not None and not df.empty:
                wide = df.pivot_table(index="time", columns="code", values="money")
                wide.columns = [to_pt(c) for c in wide.columns]
                return wide.sort_index()
        except Exception:
            pass
        field = "money"
        col = "money"
    out = {}
    # 批量路径：多标的 + 已预加载内存缓存，直接切片（镜像 jq api，避免逐只 fetch/回源）
    if len(engine_codes) > 1:
        mem = mgr._daily_mem if freq == "1d" else mgr._minute_mem
        if mem:
            for pt_code, ec in zip(codes, engine_codes, strict=False):
                try:
                    raw = mem.get(f"get_daily_{ec}") if freq == "1d" else mem.get(ec)
                    if raw is None or (hasattr(raw, "empty") and raw.empty):
                        continue
                    if col not in raw.columns:
                        continue
                    sub = raw[col]
                    if isinstance(raw.index, pd.DatetimeIndex) and now is not None:
                        if freq == "1d":
                            if now.hour < 15:
                                sub = sub[raw.index.normalize() < now.normalize()]
                        else:
                            sub = sub[raw.index <= now]
                    if count and len(sub) > count:
                        sub = sub.iloc[-int(count):]
                    if sub.empty:
                        continue
                    out[pt_code] = pd.to_numeric(sub, errors="coerce")
                except Exception:
                    continue
            if out:
                return pd.DataFrame(out).sort_index()
    # 逐只路径（单标的 get_history 走这里；优先读内存缓存避免回源）
    for pt_code, ec in zip(codes, engine_codes, strict=False):
        try:
            raw = None
            if freq == "1m":
                raw = getattr(mgr, "_minute_mem", {}).get(ec)
                if raw is None or (hasattr(raw, "empty") and raw.empty):
                    raw = mgr.get_minute(ec, str(now.date())[:10] if now is not None else None, None)
            else:
                raw = mgr.fetch("get_daily", ec, "20000101", "20300101")
            if raw is None or (hasattr(raw, "empty") and raw.empty):
                continue
            sub = raw
            if isinstance(raw.index, pd.DatetimeIndex) and now is not None:
                if freq == "1d":
                    if now.hour < 15:
                        sub = raw[raw.index.normalize() < now.normalize()]
                else:
                    sub = raw[raw.index <= now]
            if count and len(sub) > count:
                sub = sub.iloc[-int(count):]
            if sub.empty:
                continue
            if col not in sub.columns:
                continue
            s = pd.to_numeric(sub[col], errors="coerce")
            out[pt_code] = s
        except Exception:
            continue
    if not out:
        return pd.DataFrame()
    df = pd.DataFrame(out).sort_index()
    # 官方 get_history 单标的（str）列=行情字段；多标的宽表（代码列）=官方 py3.5 变体。
    # 单标的把列名设为字段名，使策略可用 df[field] 取列（与真 PTrade 一致）。
    if len(codes) == 1 and len(df.columns) == 1 and df.columns[0] not in _PT_FIELDS:
        df.columns = [col]
    return df


def get_stock_status(codes, query_type="HALT", query_date=None):
    """停牌检测：HALT → {PTrade码: 是否停牌}。由 minute_prices 可得性推导。"""
    if isinstance(codes, str):
        codes = [codes]
    out = {}
    snap = _state.get("minute_prices") or {}
    for c in codes:
        out[c] = c not in snap
    return out


# ---- 交易日历（DataManager 指数日线推导） ----
_A_SHARE_CALENDAR = None


def _get_calendar():
    global _A_SHARE_CALENDAR
    if _A_SHARE_CALENDAR is not None:
        return _A_SHARE_CALENDAR
    mgr = _state.get("manager")
    if mgr:
        try:
            df = mgr.fetch("get_daily", "000300.XSHG", "20000101", "20300101")
            if df is not None and not (hasattr(df, "empty") and df.empty):
                _A_SHARE_CALENDAR = pd.DatetimeIndex(sorted(df.index.normalize()))
                return _A_SHARE_CALENDAR
        except Exception:
            pass
    _A_SHARE_CALENDAR = pd.bdate_range("2000-01-01", "2030-12-31")
    return _A_SHARE_CALENDAR


def _now_ts():
    ctx = _state.get("ctx")
    if ctx is not None and ctx.current_dt is not None:
        return pd.Timestamp(ctx.current_dt).normalize()
    return pd.Timestamp.now().normalize()


def get_trading_day(count=-1):
    """PTrade get_trading_day(count)：count=-1 返回前一交易日（date）。"""
    cal = _get_calendar()
    idx = int(cal.searchsorted(_now_ts()))
    if count == -1:
        return cal[max(0, idx - 1)].to_pydatetime() if idx > 0 else cal[0].to_pydatetime()
    if count == 1:
        return cal[idx].to_pydatetime() if idx < len(cal) else cal[-1].to_pydatetime()
    if count > 1:
        return cal[max(0, idx - count + 1):idx + 1][-1].to_pydatetime()
    return cal[idx].to_pydatetime() if idx < len(cal) else cal[-1].to_pydatetime()


def get_trade_days(start_date=None, end_date=None, count=None):
    """PTrade get_trade_days：返回区间内交易日（date 列表）。"""
    cal = _get_calendar()
    if end_date is not None:
        cal = cal[cal <= pd.Timestamp(end_date)]
    if start_date is not None:
        cal = cal[cal >= pd.Timestamp(start_date)]
    if count is not None:
        cal = cal[-int(count):]
    return [d.to_pydatetime() for d in cal]


def get_stock_name(stocks):
    """官方 get_stock_name(stocks)：单/多标的，返回 {code: name}。"""
    codes = [stocks] if isinstance(stocks, str) else list(stocks)
    return {c: _resolve_name(c) for c in codes}


def _name_map():
    """{6位代码: 名称}，与本地 jq 引擎同源（聚宽名快照优先，回退通达信名）。"""
    cache = _state.setdefault("_name_map_cache", {})
    if cache:
        return cache
    mgr = _state.get("manager")
    try:
        from app.quant.jqengine.engine.jq.api import _name_source
        if _name_source() == "jq":
            from app.quant.jqengine.engine.jq.jq_names import load_jq_names
            for jq_code, name in (load_jq_names() or {}).items():
                if name:
                    cache.setdefault(jq_code.split(".")[0], str(name))
    except Exception:
        pass
    if mgr:
        try:
            src = getattr(mgr, "sources", {}).get("network")
            if src is not None and hasattr(src, "get_stock_names"):
                for pure, name in (src.get_stock_names() or {}).items():
                    if name:
                        cache.setdefault(str(pure), str(name))
        except Exception:
            pass
    return cache


def _resolve_name(code):
    """标的名称：优先网络名称映射，失败回退代码。"""
    pure = code.split(".")[0]
    return _name_map().get(pure, code)


def get_market_list():
    return pd.DataFrame([{"finance_mic": "ALL"}])


def get_etf_list():
    """官方 get_etf_list：返回全部 ETF 代码列表（PTrade 码 .SS/.SZ，与聚宽 get_all_securities(['etf']) 同性质）。"""
    mgr = _state.get("manager")
    out = []
    if mgr:
        try:
            etfs = mgr.fetch("get_etf_list") or []
            for item in etfs:
                if isinstance(item, str):
                    jq = str(item).replace(".SH", ".XSHG").replace(".SZ", ".XSHE")
                    out.append(to_pt(jq))
                elif isinstance(item, dict):
                    ts = str(item.get("ts_code", ""))
                    jq = ts.replace(".SH", ".XSHG").replace(".SZ", ".XSHE")
                    if ".X" in jq:
                        out.append(to_pt(jq))
        except Exception:
            pass
    return out


def get_market_detail(mic):
    """全市场基金表：prod_code(PTrade)/prod_name。DataManager etf 名录（字符串或 dict）。
    prod_name 用网络名称映射（对齐 jq get_all_securities），缺失回退代码。"""
    mgr = _state.get("manager")
    names = _name_map()
    rows = []
    if mgr:
        try:
            etfs = mgr.fetch("get_etf_list") or []
            for item in etfs:
                if isinstance(item, str):
                    ts = item
                    name = names.get(item.split(".")[0], item)
                elif isinstance(item, dict):
                    ts = str(item.get("ts_code", ""))
                    name = names.get(ts.split(".")[0], str(item.get("name", "") or ts))
                else:
                    continue
                if "." not in ts:
                    continue
                # ts 为 SH/SZ 格式 → 先转 JQ 再转 PTrade（SH→SS）
                jq = str(ts).replace(".SH", ".XSHG").replace(".SZ", ".XSHE")
                rows.append({"prod_code": to_pt(jq), "prod_name": name})
        except Exception:
            pass
    return pd.DataFrame(rows)


def set_benchmark(code):
    _state["benchmark"] = code


def set_commission(commission_ratio=None, min_commission=None, type=None, **kw):  # noqa: A002
    """PTrade 佣金：存入 fee_config（matcher/order 读取）。"""
    if commission_ratio is not None:
        _state["fee"] = float(commission_ratio)
        cfg = dict(_state.get("fee_config") or {})
        cfg["open_commission"] = float(commission_ratio)
        cfg["close_commission"] = float(commission_ratio)
        cfg["close_today_commission"] = float(commission_ratio)
        cfg["min_commission"] = float(min_commission or 0)
        _state["fee_config"] = cfg


def set_slippage(slippage=0.0):
    _state["slippage"] = float(slippage)


class _LogProxy:
    _levels: ClassVar[dict[str, int]] = {"debug": 0, "info": 1, "warn": 2, "error": 3}
    _modules: ClassVar[dict] = {}

    def set_level(self, module, level):
        self._modules[module] = self._levels.get(level, 1)

    def _should(self, level_name):
        return self._modules.get("strategy", 1) <= self._levels[level_name]

    def _emit(self, level, msg):
        sink = _state.get("log_sink")
        if sink is not None:
            from contextlib import suppress
            with suppress(Exception):
                sink(level, msg)

    def info(self, msg):
        self._emit("info", msg)

    def warn(self, msg):
        self._emit("warn", msg)

    def warning(self, msg):
        self.warn(msg)

    def error(self, msg):
        self._emit("error", msg)

    def debug(self, msg):
        self._emit("debug", msg)

    def notify(self, msg):
        self.info(msg)
        self._emit("notify", msg)


log = _LogProxy()


def record(**kw):
    _state["records"].append(kw)


def get_price(security, start_date=None, end_date=None, frequency="1d",
              fields=None, fq="pre", count=None, is_dict=False):
    """docx 原生 get_price：复用 get_history 口径。返回宽表 DataFrame。"""
    fields = fields or ["close"]
    if isinstance(fields, str):
        fields = [fields]
    codes = [security] if isinstance(security, str) else list(security)
    field_out = {}
    for f in fields:
        df = get_history(count, frequency, f, security_list=codes, include=False, fq=fq,
                         end_dt=end_date if end_date is not None else None)
        if df is None or df.empty:
            continue
        if start_date is not None:
            df = df[df.index >= pd.Timestamp(start_date)]
        field_out[f] = df
    if not field_out:
        return pd.DataFrame()
    if len(fields) == 1:
        df = field_out[fields[0]]
        if len(codes) == 1 and len(df.columns) == 1:
            df = df.copy()
            df.columns = [fields[0]]
        return df
    # 多字段：返回 {field: DataFrame} 的 dict（docx py3.5 panel 变体简化）
    return field_out


def _last_field(df, field):
    """get_history 单标的单字段结果取最新值（列名=字段名，兼容代码列名回退）。"""
    if df is None or df.empty:
        return 0.0
    if field in df.columns:
        return float(df[field].iloc[-1])
    return float(df.iloc[-1, 0])


def check_limit(security, query_date=None):
    """docx check_limit：{码: int}，-2 触板跌停/-1 跌停/0 平/1 涨停/2 触板涨停。
    本地引擎用日线 high_limit/low_limit 字段 + 最新价推导。"""
    codes = [security] if isinstance(security, str) else list(security)
    out = {}
    for c in codes:
        out[c] = 0
        try:
            close = get_history(1, "1d", "close", security_list=c, include=True)
            high = get_history(1, "1d", "high_limit", security_list=c, include=True)
            low = get_history(1, "1d", "low_limit", security_list=c, include=True)
            last = _last_field(close, "close")
            hp = _last_field(high, "high_limit")
            lp = _last_field(low, "low_limit")
            if hp and last >= hp:
                out[c] = 1
            elif lp and last <= lp:
                out[c] = -1
        except Exception:
            continue
    return out


def get_stock_info(stocks, field=None):
    """docx get_stock_info：{码: {stock_name, listed_date, de_listed_date}}。"""
    codes = [stocks] if isinstance(stocks, str) else list(stocks)
    fields = ["stock_name", "listed_date", "de_listed_date"] if field is None else (
        [field] if isinstance(field, str) else list(field))
    out = {}
    for c in codes:
        out[c] = {f: ("" if f != "stock_name" else _resolve_name(c)) for f in fields}
    return out


def get_snapshot(security):
    """docx get_snapshot：{码: {up_px, down_px, last_px, ...}}。回测回退日线字段。"""
    codes = [security] if isinstance(security, str) else list(security)
    out = {}
    for c in codes:
        snap = {"last_px": 0.0, "up_px": 0.0, "down_px": 0.0,
                "preclose_px": 0.0, "high_px": 0.0, "low_px": 0.0,
                "business_balance": 0.0, "volume": 0, "amount": 0}
        try:
            close = get_history(1, "1d", "close", security_list=c, include=True)
            high = get_history(1, "1d", "high_limit", security_list=c, include=True)
            low = get_history(1, "1d", "low_limit", security_list=c, include=True)
            preclose = get_history(1, "1d", "preclose", security_list=c, include=True)
            snap["last_px"] = _last_field(close, "close")
            snap["up_px"] = _last_field(high, "high_limit")
            snap["down_px"] = _last_field(low, "low_limit")
            snap["preclose_px"] = _last_field(preclose, "preclose")
        except Exception:
            pass
        out[c] = snap
    return out


def order_target(security, amount, limit_price=None):
    """docx order_target：调整持仓到目标数量。"""
    cur = get_position(security).amount
    diff = int(amount) - int(cur)
    if diff == 0:
        return True
    return order(security, diff) if diff > 0 else order(security, -min(abs(diff), get_position(security).enable_amount))


def order_target_value(security, value, limit_price=None):
    """docx order_target_value：调整持仓到目标市值。本地引擎用 order_value 近似（差异仅整手尾差）。"""
    price = _live_price(security)
    if price == 0:
        return False
    cur_val = get_position(security).amount * price
    diff = float(value) - cur_val
    if abs(diff) < price * 100:
        return True
    return order_value(security, diff)


def get_all_trades_days(date=None):
    """docx get_all_trades_days：date 之前的全部交易日（含 date）。"""
    cal = _get_calendar()
    if date is not None:
        cal = cal[cal <= pd.Timestamp(date)]
    return [d.to_pydatetime().date() for d in cal]


def get_trading_day_by_date(query_date, day=0):
    """docx get_trading_day_by_date：按日期偏移交易日。"""
    cal = _get_calendar()
    ts = pd.Timestamp(query_date)
    idx = int(cal.searchsorted(ts))
    target = idx + int(day)
    if target < 0:
        target = 0
    if target >= len(cal):
        target = len(cal) - 1
    return cal[target].to_pydatetime().date()


def get_etf_info(stocks):
    """docx get_etf_info：{码: 名称}。复用 get_stock_name。"""
    return get_stock_name(stocks)


def build_data_snapshot(ctx):
    """组装 handle_data / before_trading_start 的行情快照 {PTrade码: SimpleNamespace}。

    价格源：minute_prices（PTrade 域，runner 每 bar 写入）；OHLC 取 manager 日线
    最近 bar。策略 _BarUnit 主要读 price/close/volume/money。"""
    mgr = _state.get("manager")
    snap = _state.get("minute_prices") or {}
    codes = set(snap.keys())
    for code in getattr(ctx, "universe", []) or []:
        codes.add(code)
    for code in getattr(ctx.portfolio, "positions", {}) or {}:
        codes.add(code)
    out = {}
    for code in codes:
        price = snap.get(code)
        if not price and mgr and ctx and ctx.current_dt is not None:
            try:
                price = mgr.get_minute_price_at(to_engine(code), ctx.current_dt)
            except Exception:
                price = None
        out[code] = types.SimpleNamespace(
            code=code, dt=getattr(ctx, "current_dt", None),
            open=0.0, high=0.0, low=0.0,
            close=float(price or 0.0), price=float(price or 0.0),
            volume=0, money=0.0, name=None, paused=False,
            highLimit=None, lowLimit=None,
        )
    return out
