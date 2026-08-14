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

_DAILY_AT = {}          # (hour, minute) -> [func]（同一时刻按注册顺序触发）
_EVERY_BAR_CALLBACKS = []
_UNIVERSE = []          # JQ 码（get_history 缺省 security_list 时用）
_NAMES = {}             # JQ 码 -> name
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
# 以下 API 在后续 Task 实现（当前占位，避免 install 时 NameError）
# ---------------------------------------------------------------------------
def order(code, amount):
    raise NotImplementedError


def get_position(code):
    raise NotImplementedError


def get_positions():
    raise NotImplementedError


def get_trading_day(count=-1):
    raise NotImplementedError


def get_trade_days(start_date=None, end_date=None, count=None):
    raise NotImplementedError


def set_universe(codes):
    raise NotImplementedError


def get_stock_status(codes, query_type="HALT", query_date=None):
    raise NotImplementedError


def get_stock_name(code):
    raise NotImplementedError


def get_market_list():
    raise NotImplementedError


def get_market_detail(mic):
    raise NotImplementedError


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
