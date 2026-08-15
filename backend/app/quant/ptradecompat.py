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
_MARKET_CODES: list = []                      # 全市场 ETF JQ 码（install 传入，与聚宽名单一致）
_BENCHMARK = "510300.SS"
_ACTIVE = False                              # 本进程是否处于 ptrade 回测模式（quantbridge 路由用）
_NATIVE_ORDER = None                         # rqalpha 原生 order（注册 ptrade order 前捕获，防自递归）


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
    # 日线盘中（<15:00）回退到前一交易日：聚宽/PTrade 日线历史不含"未完成的当日"，
    # 且 qfq 前复权因子按该日锚定（含当日会取到次日分拆/分红因子，历史价错位）。
    if freq == "1d":
        _now = pd.Timestamp(getattr(env, "trading_dt", end_dt))
        if pd.Timestamp(end_dt) >= _now.normalize() \
                and (_now.hour, _now.minute, _now.second) < (15, 0, 0):
            try:
                end_dt = pd.Timestamp(env.data_proxy.get_previous_trading_date(_now.date()))
            except Exception:  # noqa: BLE001
                end_dt = _now.normalize() - pd.Timedelta(days=1)
    actual_field = "total_turnover" if field == "money" else field
    # 修正后的元成交额（引擎专用字段）：对齐聚宽 get_daily_money_cached 口径
    # （本地日线 money 列单位需经 _ensure_money_yuan 修正，history_bars 的
    # total_turnover=close×volume 在部分 ETF 上被放大）。真 PTrade 无此字段，
    # 策略回退 'money'。
    if field == "money_corrected":
        try:
            dm = getattr(env.data_source, "_dm", None)
            if dm is not None and hasattr(dm, "get_daily_money_cached"):
                df = dm.get_daily_money_cached(jq_codes, str(pd.Timestamp(end_dt).date()), int(count))
                if df is not None and not df.empty:
                    wide = df.pivot_table(index="time", columns="code", values="money")
                    wide.columns = [_to_pt(c) for c in wide.columns]
                    return wide.sort_index()
        except Exception:  # noqa: BLE001
            pass
        # 回退：按普通 'money' 处理（真 PTrade 无 money_corrected 字段）
        field = "money"
        actual_field = "total_turnover"
    try:
        bars = _history_bars_batch(jq_codes, int(count), freq, [actual_field], end_dt)
    except Exception as e:  # noqa: BLE001
        logger.debug("get_history 批量失败，回退逐只: %s", e)
        bars = {}
        for jc in jq_codes:
            try:
                arr = env.data_source.history_bars(jc, int(count), freq, [actual_field])
                if arr is not None and len(arr):
                    bars[jc] = arr
            except Exception:  # noqa: BLE001
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

                # user=True：放在 _listeners（含 Strategy.handle_bar → handle_data
                # 更新策略 _LAST_DATA 快照）之后触发，保证 run_daily 回调读取到的是
                # 当前 bar 的快照而非上一根（否则 13:10 回调拿到 13:09 价、股数错位）。
                env.event_bus.add_listener(EVENT.BAR, _on_bar, user=True)
                if self._is_daily:
                    env.event_bus.add_listener(
                        EVENT.BEFORE_TRADING, lambda e: _fire((9, 31)), user=True)
                    env.event_bus.add_listener(
                        EVENT.AFTER_TRADING, lambda e: _fire(None, exclude=(9, 31)), user=True)

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
    if _NATIVE_ORDER is None:
        raise RuntimeError("ptradecompat.order: 未捕获 rqalpha 原生 order")
    return _NATIVE_ORDER(_to_jq(code), int(amount))


def _pos_fields(pos):
    """rqalpha Position/PositionProxy → PTrade 字段视图。

    兼容两类对象：jqcompat patch 后的 PositionProxy（total_amount/closeable_amount/
    avg_cost/price）与核心 Position（quantity/closable/avg_price/last_price），
    按名依次取，全缺回退 0。"""
    def _f(*names):
        for n in names:
            try:
                v = getattr(pos, n, None)
                if v is not None:
                    return float(v)
            except Exception:  # noqa: BLE001
                continue
        return 0.0
    return types.SimpleNamespace(
        amount=_f("total_amount", "amount", "quantity"),
        enable_amount=_f("closeable_amount", "enable_amount", "closable"),
        cost_basis=_f("avg_cost", "cost_basis", "cost", "avg_price"),
        last_sale_price=_f("price", "last_sale_price", "last_price"))


def get_position(code):
    from rqalpha.const import POSITION_DIRECTION
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    jq = _to_jq(code)
    try:
        pos = env.portfolio.get_position(jq, POSITION_DIRECTION.LONG)
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
        items = list(env.portfolio.get_positions())
    except Exception:  # noqa: BLE001
        items = []
    for pos in items:
        jq = getattr(pos, "order_book_id", None)
        if not jq:
            continue
        fields = _pos_fields(pos)
        if fields.amount > 0:
            out[_to_pt(jq)] = _position_view(fields, _to_pt(jq))
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
    """PTrade set_universe：只把数据源已知的标的加入 rqalpha universe。
    策略动态池可能含数据源未注册的代码，直接 update_universe 会抛
    RQInvalidInstrument 导致整轮回调失败。"""
    from rqalpha.api import update_universe
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    if isinstance(codes, str):
        codes = [codes]
    known = []
    for c in codes:
        try:
            ins = env.data_proxy.get_instrument(_to_jq(c))
            if ins is not None:
                known.append(_to_jq(c))
        except Exception:  # noqa: BLE001
            continue
    if known:
        update_universe(known)


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
    jq = _to_jq(code)
    if jq in _NAMES:
        return {code: _NAMES[jq]}
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    try:
        instr = env.data_proxy.get_instrument(jq)
        return {code: getattr(instr, "symbol", None) or code}
    except Exception:  # noqa: BLE001
        return {code: code}


def get_market_list():
    """PTrade get_market_list：单行 'ALL' 市场（配合 get_market_detail 全市场枚举）。"""
    return pd.DataFrame([{"finance_mic": "ALL"}])


def get_market_detail(mic):
    """全市场基金表：prod_code(PTrade)/prod_name。

    代码集合用 install_ptradecompat 传入的 market_codes（= _load_etf_universe 的
    全市场 ETF 名单，与聚宽 get_all_securities(['etf']) 逐只一致）；不依赖
    data_proxy.get_instruments 的运行时注册表（其返回集合与回测/动态池口径
    有偏差，会导致流动性阈值与池成员分歧）。
    prod_name 优先用 _NAMES（install 传入的真实 ETF 名）。"""
    rows = []
    for jq in _MARKET_CODES:
        name = _NAMES.get(jq) or jq
        rows.append({"prod_code": _to_pt(jq), "prod_name": str(name)})
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
def _register_ptrade_apis():
    """把 PTrade shim 注册进 rqalpha.api（quantbridge start_up 时兜底重注册，
    防止 sys_* mod 原生实现覆盖）。"""
    global _NATIVE_ORDER
    from rqalpha.api import register_api
    # 捕获 rqalpha 原生 order 引用：注册 ptrade order 前，避免后续自递归
    # （ptrade order 内部需调用原生撮合）。
    try:
        import rqalpha.api as _ra
        _cur = getattr(_ra, "order", None)
        if _cur is not None and _cur is not order:
            _NATIVE_ORDER = _cur
    except Exception:  # noqa: BLE001
        pass
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


def install_ptradecompat(universe, names=None, benchmark="510300.SS", list_dates=None,
                         market_codes=None):
    global _UNIVERSE, _NAMES, _BENCHMARK, _ACTIVE, _MARKET_CODES
    _UNIVERSE = [str(c) for c in universe]           # JQ 码
    _NAMES = dict(names or {})                       # JQ 码 -> name
    _BENCHMARK = benchmark
    _MARKET_CODES = [str(c) for c in (market_codes or universe)]  # 全市场 ETF JQ 码
    _DAILY_AT.clear()
    _EVERY_BAR_CALLBACKS.clear()
    _ACTIVE = True
    _register_ptrade_apis()
    _patch_rqalpha_objects()
    _install_barcache_mod()
