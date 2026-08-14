"""PTrade → rqalpha 6.2.1 兼容层（镜像 jqcompat）。

让 PTrade 风格策略（def initialize / before_trading_start / after_trading_end /
handle_data + run_daily(context, func, time) + get_history/order/get_positions）
直接在 rqalpha 上回测。strategy 侧全部 PTrade 代码（.SS/.SZ），内部与 rqalpha
（order_book_id .XSHG/.XSHE）交界处转换。
"""
from __future__ import annotations

import logging
import sys
import types
from typing import ClassVar

import numpy as np
import pandas as pd

logger = logging.getLogger("ptradecompat")

_DAILY_AT: dict[tuple[int, int], list] = {}   # (hour, minute) -> [func]（同一时刻按注册顺序触发）
_EVERY_BAR_CALLBACKS: list = []
_UNIVERSE: list = []                          # JQ 码（get_history 缺省 security_list 时用）
_NAMES: dict = {}                             # JQ 码 -> name
_BENCHMARK = "510300.SS"


# ---------------------------------------------------------------------------
# 代码域转换（PTrade .SS/.SZ <-> rqalpha/JQ .XSHG/.XSHE）
# ---------------------------------------------------------------------------
def _to_jq(code):
    return str(code).replace(".SS", ".XSHG").replace(".SZ", ".XSHE")


def _to_pt(code):
    return str(code).replace(".XSHG", ".SS").replace(".XSHE", ".SZ")


def _norm_freq(freq):
    freq = (freq or "1d").lower()
    if freq in ("daily", "day", "1d"):
        return "1d"
    if freq in ("min", "minute", "1m"):
        return "1m"
    return freq


# ---------------------------------------------------------------------------
# get_history：多标的宽表（index=datetime, columns=PTrade 码）
# ---------------------------------------------------------------------------
def _build_history_wide(bars, jq_codes, field):
    """把 history_bars_batch 结果（{jq_code: np.struct}）组装为宽表。
    index=datetime，columns=PTrade 码，值为 field。"""
    out = {}
    for code in jq_codes:
        arr = bars.get(code)
        if arr is None or len(arr) == 0:
            continue
        times = pd.to_datetime(np.asarray(arr["datetime"]).astype(str),
                               format="%Y%m%d%H%M%S")
        actual = "total_turnover" if field == "money" else field
        vals = np.asarray(arr[actual]) if actual in arr.dtype.names \
            else np.full(len(arr), np.nan)
        out[_to_pt(code)] = pd.Series(vals, index=times)
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out).sort_index()


def _history_bars_batch(codes, count, freq, fields, end_dt):
    """批量取数（独立函数便于测试 monkeypatch）。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    return env.data_source.history_bars_batch(codes, count, freq, fields, end_dt)


def get_history(count, frequency, field, security_list=None, include=True, fq="pre"):
    """PTrade get_history：单/多标的宽表。security_list 缺省用 _UNIVERSE。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    if security_list is None:
        codes = list(_UNIVERSE)
    elif isinstance(security_list, str):
        codes = [security_list]
    else:
        codes = list(security_list)
    jq_codes = [_to_jq(c) for c in codes]
    freq = _norm_freq(frequency)
    end_dt = getattr(env, "trading_dt", None) or pd.Timestamp.now()
    try:
        bars = _history_bars_batch(jq_codes, int(count), freq, [field], end_dt)
    except Exception as e:
        logger.debug("get_history 批量失败，回退逐只: %s", e)
        bars = {}
        for jc in jq_codes:
            try:
                arr = env.data_source.history_bars(jc, int(count), freq, [field])
                if arr is not None and len(arr):
                    bars[jc] = arr
            except Exception:
                continue
    df = _build_history_wide(bars, jq_codes, field)
    if include and not df.empty:
        df = df[df.index <= pd.Timestamp(end_dt)]
    return df


# ---------------------------------------------------------------------------
# 调度：run_daily(context, func, time) / every_bar
# ---------------------------------------------------------------------------
def run_daily(context, func, time="HH:MM"):
    """PTrade run_daily：time='HH:MM' 或 'every_bar'。回调签名 func(context)。"""
    if time == "every_bar":
        _EVERY_BAR_CALLBACKS.append(func)
        return
    try:
        hh, mm = str(time).split(":")
        hm = (int(hh), int(mm))
    except Exception:
        hm = (9, 31)
    _DAILY_AT.setdefault(hm, []).append(func)


# ---------------------------------------------------------------------------
# bar_dict 适配：rqalpha BarDict -> {PTrade 码: SecurityUnitData 替身}
# ---------------------------------------------------------------------------
def _ptrade_adapt_bar_dict(bar_dict):
    """rqalpha BarDict → {PTrade码: SecurityUnitData 替身}。"""
    out = {}
    if not bar_dict:
        return out
    for code, bar in (bar_dict.items() if hasattr(bar_dict, "items") else []):
        try:
            out[_to_pt(code)] = types.SimpleNamespace(
                code=_to_pt(code), dt=getattr(bar, "datetime", None),
                open=getattr(bar, "open", None), high=getattr(bar, "high", None),
                low=getattr(bar, "low", None), close=getattr(bar, "close", None),
                price=getattr(bar, "close", None), volume=getattr(bar, "volume", None),
                money=getattr(bar, "total_turnover", None), name=None)
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# rqalpha mod：按 (hour,minute) 触发 run_daily 回调（镜像 jqbarcache）
# ---------------------------------------------------------------------------
def _install_barcache_mod():
    if "rqalpha_mod_ptradebarcache" in sys.modules:
        return
    mod = types.ModuleType("rqalpha_mod_ptradebarcache")

    def load_mod():
        from rqalpha.core.events import EVENT
        from rqalpha.environment import Environment  # noqa: F401
        from rqalpha.interface import AbstractMod

        class _PtradeBarCacheMod(AbstractMod):
            def start_up(self, env, mod_config):
                _base_cfg = getattr(getattr(env, "config", None), "base", None)
                _freq = str(getattr(_base_cfg, "frequency", "1m") or "1m")
                self._is_daily = _freq == "1d"

                def _uctx():
                    return getattr(getattr(env, "user_strategy", None),
                                   "user_context", None)

                def _fire(hm_key=None, exclude=None):
                    uctx = _uctx()
                    items = [(hm_key, _DAILY_AT.get(hm_key, []))] if hm_key is not None \
                        else list(_DAILY_AT.items())
                    for hm, cbs in items:
                        if exclude is not None and hm == exclude:
                            continue
                        for cb in list(cbs):
                            try:
                                cb(uctx)
                            except Exception as e:
                                logger.warning("daily_at(%s) 回调异常: %s", hm, e)

                def _on_bar(event):
                    uctx = _uctx()
                    for cb in list(_EVERY_BAR_CALLBACKS):
                        try:
                            cb(uctx)
                        except Exception as e:
                            logger.debug("every_bar 回调异常: %s", e)
                    if self._is_daily:
                        return
                    dt = getattr(env, "trading_dt", None)
                    hm = (dt.hour, dt.minute) if dt is not None else None
                    if hm is not None:
                        for cb in list(_DAILY_AT.get(hm, [])):
                            try:
                                cb(uctx)
                            except Exception as e:
                                logger.warning("daily_at(%s) 回调异常: %s", hm, e)

                env.event_bus.add_listener(EVENT.BAR, _on_bar)
                if self._is_daily:
                    env.event_bus.add_listener(
                        EVENT.BEFORE_TRADING, lambda e: _fire((9, 31)))
                    env.event_bus.add_listener(
                        EVENT.AFTER_TRADING, lambda e: _fire(None, exclude=(9, 31)))

            def tear_down(self, *args):
                return

        return _PtradeBarCacheMod()

    mod.load_mod = load_mod
    mod.__config__ = {"base": {}, "mod": {}, "extra": {}}
    sys.modules["rqalpha_mod_ptradebarcache"] = mod


# ---------------------------------------------------------------------------
# rqalpha 对象补丁：context.blotter / portfolio.portfolio_value
# ---------------------------------------------------------------------------
def _patch_rqalpha_objects():
    try:
        from rqalpha.core.strategy_context import StrategyContext
        from rqalpha.environment import Environment
        from rqalpha.portfolio import Portfolio
    except Exception:
        return
    if not hasattr(StrategyContext, "blotter"):
        def _blotter(self):
            env = Environment.get_instance()
            return types.SimpleNamespace(current_dt=getattr(env, "calendar_dt", None))
        StrategyContext.blotter = property(_blotter)
    if not hasattr(Portfolio, "portfolio_value"):
        Portfolio.portfolio_value = property(lambda self: self.total_value)


# ---------------------------------------------------------------------------
# 订单 / 持仓 / 交易日 / 停牌 / 名称 / 市场枚举
# ---------------------------------------------------------------------------
def _position_view(pos, pt_code):
    """包装 rqalpha 持仓为 PTrade 字段视图（空仓返回 0 占位）。"""
    return types.SimpleNamespace(
        amount=float(getattr(pos, "amount", 0) or 0),
        enable_amount=float(getattr(pos, "enable_amount", 0) or 0),
        cost_basis=float(getattr(pos, "cost_basis", 0) or 0),
        last_sale_price=float(getattr(pos, "last_sale_price", 0) or 0),
        sid=pt_code, security=pt_code)


def order(code, amount):
    from rqalpha.api import order as rq_order
    return rq_order(_to_jq(code), int(amount))


def _pos_fields(pos):
    return types.SimpleNamespace(
        amount=getattr(pos, "amount", 0) or 0,
        enable_amount=getattr(pos, "enable_amount", 0) or 0,
        cost_basis=getattr(pos, "cost_basis", 0) or 0,
        last_sale_price=getattr(pos, "last_sale_price", 0) or 0)


def get_position(code):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    jq = _to_jq(code)
    try:
        pos = env.portfolio.get_position(jq)
    except Exception:  # noqa: BLE001
        pos = None
    if pos is None:
        return _position_view(None, code)
    return _position_view(_pos_fields(pos), code)


def get_positions():
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    out = {}
    try:
        items = list(env.portfolio.positions.items())
    except Exception:  # noqa: BLE001
        items = []
    for jq, pos in items:
        amount = float(getattr(pos, "amount", 0) or 0)
        if amount > 0:
            out[_to_pt(jq)] = _position_view(_pos_fields(pos), _to_pt(jq))
    return out


def _prev_trading_day(date):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    try:
        return pd.Timestamp(env.data_proxy.get_previous_trading_date(date))
    except Exception:  # noqa: BLE001
        return pd.Timestamp(date) - pd.Timedelta(days=1)


def get_trading_day(count=-1):
    """PTrade get_trading_day(count)：count=-1 返回前一交易日。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    today = pd.Timestamp(getattr(env, "trading_dt", None) or pd.Timestamp.now())
    if count == -1:
        return _prev_trading_day(today.date())
    if count == 1:
        return today.normalize()
    if count > 1:
        try:
            cal = env.data_proxy.get_trading_calendar()
            idx = cal.searchsorted(today.normalize())
            return list(cal[max(0, idx - count + 1):idx + 1])[-1]
        except Exception:  # noqa: BLE001
            return today.normalize()
    return today.normalize()


def get_trade_days(start_date=None, end_date=None, count=None):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    cal = env.data_proxy.get_trading_calendar()
    if end_date is not None:
        cal = cal[cal <= pd.Timestamp(end_date)]
    if start_date is not None:
        cal = cal[cal >= pd.Timestamp(start_date)]
    if count is not None:
        cal = cal[-int(count):]
    return list(cal)


def set_universe(codes):
    from rqalpha.api import update_universe
    if isinstance(codes, str):
        codes = [codes]
    update_universe([_to_jq(c) for c in codes])


def get_stock_status(codes, query_type="HALT", query_date=None):
    """停牌检测：HALT → {PTrade码: 是否停牌}。失败返回空（策略容错为不判定）。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    if isinstance(codes, str):
        codes = [codes]
    out = {}
    for c in codes:
        try:
            out[c] = bool(env.data_proxy.is_suspended(_to_jq(c), query_date))
        except Exception:  # noqa: BLE001
            continue
    return out


def get_stock_name(code):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    try:
        instr = env.data_proxy.instruments(_to_jq(code))
        return {code: getattr(instr, "symbol", None) or code}
    except Exception:  # noqa: BLE001
        return {code: code}


def get_market_list():
    """PTrade get_market_list：单行 'ALL' 市场（配合 get_market_detail 全市场枚举）。"""
    return pd.DataFrame([{"finance_mic": "ALL"}])


def get_market_detail(mic):
    """全市场基金表：rqalpha all_instruments(type='etf') → prod_code(PTrade)/prod_name。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    rows = []
    try:
        df = env.data_proxy.all_instruments(type="etf")
    except Exception:  # noqa: BLE001
        df = None
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame()
    for _, r in df.iterrows():
        jq = str(r.get("order_book_id", ""))
        if not jq:
            continue
        rows.append({"prod_code": _to_pt(jq), "prod_name": str(r.get("symbol", "") or jq)})
    return pd.DataFrame(rows)


def set_benchmark(code):
    global _BENCHMARK
    _BENCHMARK = code


def set_commission(commission_ratio=None, min_commission=None, type=None, **kw):
    """PTrade 佣金（回测经 rqalpha 配置生效，此处存储式 no-op）。"""
    return None


def set_slippage(slippage=0.0):
    """PTrade 滑点（回测经 rqalpha 配置生效，此处存储式 no-op）。"""
    return None


class _LogProxy:
    _levels: ClassVar[dict[str, int]] = {"debug": 0, "info": 1, "warn": 2, "error": 3}
    _cur = 1

    def set_level(self, module, level):
        self._cur = self._levels.get(level, 1)

    def _emit(self, msg):
        print(f"[PTRADE] {msg}")

    def info(self, msg):
        self._emit(msg)

    def warn(self, msg):
        self._emit(f"[WARN] {msg}")

    def warning(self, msg):
        self.warn(msg)

    def error(self, msg):
        self._emit(f"[ERROR] {msg}")

    def debug(self, msg):
        self._emit(f"[DEBUG] {msg}")

    def notify(self, msg):
        self._emit(f"[NOTIFY] {msg}")


log = _LogProxy()


# ---------------------------------------------------------------------------
# 安装入口：把 PTrade API 注册进 rqalpha 策略命名空间
# ---------------------------------------------------------------------------
def install_ptradecompat(universe, names=None, benchmark="510300.SS", list_dates=None):
    global _UNIVERSE, _NAMES, _BENCHMARK
    _UNIVERSE = [str(c) for c in universe]           # JQ 码
    _NAMES = dict(names or {})                       # JQ 码 -> name
    _BENCHMARK = benchmark
    _DAILY_AT.clear()
    _EVERY_BAR_CALLBACKS.clear()
    from rqalpha.api import register_api
    register_api("get_history", get_history)
    register_api("run_daily", run_daily)
    register_api("order", order)
    register_api("get_position", get_position)
    register_api("get_positions", get_positions)
    register_api("get_trading_day", get_trading_day)
    register_api("get_trade_days", get_trade_days)
    register_api("set_universe", set_universe)
    register_api("get_stock_status", get_stock_status)
    register_api("get_stock_name", get_stock_name)
    register_api("get_market_list", get_market_list)
    register_api("get_market_detail", get_market_detail)
    register_api("set_benchmark", set_benchmark)
    register_api("set_commission", set_commission)
    register_api("set_slippage", set_slippage)
    register_api("log", log)
    register_api("_ptrade_adapt_bar_dict", _ptrade_adapt_bar_dict)
    _patch_rqalpha_objects()
    _install_barcache_mod()
