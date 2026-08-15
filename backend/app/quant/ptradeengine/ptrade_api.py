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

import pandas as pd

from .context import PtradeContext, PtradePortfolio, PtradePosition, ptrade_code_conv

DEFAULT_STAMP_TAX = 0.0005  # 卖出印花税（A股 0.05%，ETF 免征）

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


def get_history(count, frequency, field, security_list=None, include=True, fq="pre"):
    """PTrade get_history：多标的宽表（index=datetime, columns=PTrade 码）。

    数据走 DataManager（JQ 码），fq='pre' 与 jq get_price 同口径。"""
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
    out = {}
    for pt_code, ec in zip(codes, engine_codes, strict=False):
        try:
            if freq == "1m":
                raw = mgr.get_minute(ec, str(ctx.current_dt)[:10], None)
            else:
                raw = mgr.fetch("get_daily", ec, "20000101", "20300101")
            if raw is None or (hasattr(raw, "empty") and raw.empty):
                continue
            sub = raw
            if isinstance(raw.index, pd.DatetimeIndex) and ctx and ctx.current_dt is not None:
                if freq == "1d":
                    # 日线盘中（<15:00）不含当日（防前视），与 jq 口径一致
                    now = pd.Timestamp(ctx.current_dt)
                    if now.hour < 15:
                        sub = raw[raw.index.normalize() < now.normalize()]
                else:
                    sub = raw[raw.index <= pd.Timestamp(ctx.current_dt)]
            if count and len(sub) > count:
                sub = sub.iloc[-int(count):]
            if sub.empty:
                continue
            col = "total_turnover" if field == "money" else field
            if col not in sub.columns:
                continue
            s = pd.to_numeric(sub[col], errors="coerce")
            out[pt_code] = s
        except Exception:
            continue
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out).sort_index()


def get_stock_status(codes, query_type="HALT", query_date=None):
    """停牌检测：HALT → {PTrade码: 是否停牌}。由 minute_prices 可得性推导。"""
    if isinstance(codes, str):
        codes = [codes]
    out = {}
    snap = _state.get("minute_prices") or {}
    for c in codes:
        out[c] = c not in snap
    return out


def get_stock_name(code):
    name = _resolve_name(code)
    return {code: name}


def _resolve_name(code):
    """标的名称：优先 etf 名录（manager.get_etf_list），失败回退代码。"""
    mgr = _state.get("manager")
    if mgr:
        try:
            etfs = mgr.fetch("get_etf_list")
            if etfs:
                pure = code.split(".")[0]
                for item in etfs:
                    if isinstance(item, dict):
                        ts = str(item.get("ts_code", ""))
                        if ts.split(".")[0] == pure:
                            return str(item.get("name", "") or code)
        except Exception:
            pass
    return code


def get_market_list():
    return pd.DataFrame([{"finance_mic": "ALL"}])


def get_market_detail(mic):
    """全市场基金表：prod_code(PTrade)/prod_name。DataManager etf 名录。"""
    mgr = _state.get("manager")
    rows = []
    if mgr:
        try:
            etfs = mgr.fetch("get_etf_list")
            for item in etfs or []:
                if isinstance(item, dict):
                    ts = str(item.get("ts_code", ""))
                    if "." not in ts:
                        continue
                    rows.append({"prod_code": to_pt(ts),
                                 "prod_name": str(item.get("name", "") or ts)})
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
            volume=0, money=0.0, name=_resolve_name(code), paused=False,
            highLimit=None, lowLimit=None,
        )
    return out
