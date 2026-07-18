"""聚宽(jqdata) → rqalpha 6.2.1 兼容层 + 数据源。

目标：让 ``from jqdata import *`` 的策略（如五福闹新春 v5.2）在无 rqdatac 的
环境下，借助 rqalpha 自身的数据代理跑通。本模块提供：
  * 假的 ``jqdata`` 模块（使 ``from jqdata import *`` 成为 no-op）；
  * 注册缺失的聚宽 API（get_price / get_all_securities / get_current_data /
    get_security_name / attribute_history / is_temporarily_suspended /
    get_trade_days / set_benchmark / set_slippage / set_order_cost / set_option /
    history / record / run_daily / run_minute / log 等）；
  * 对 rqalpha 原生对象做兼容补丁（Position/Portfolio/StrategyContext）；
  * 一个满足 rqalpha DataProxy 需求的 ``JqDataSource``（日线 + 合成分钟线，
    通过 manager.QuantDataProvider 取数）。

所有 shim 通过 ``rqalpha.api.register_api`` 注入到策略命名空间，订单执行与事件
循环仍由 rqalpha 负责。
"""
from __future__ import annotations

import logging
import sys
import types

import numpy as np
import pandas as pd

from rqalpha.api import register_api
from rqalpha.const import INSTRUMENT_TYPE, MARKET, TRADING_CALENDAR_TYPE
from rqalpha.core.events import EVENT
from rqalpha.environment import Environment
from rqalpha.interface import AbstractMod
from rqalpha.model.instrument import Instrument

logger = logging.getLogger("jqcompat")

# ---------------------------------------------------------------------------
# 全局状态（由安装函数填充）
# ---------------------------------------------------------------------------
_CURRENT_BAR_DICT = None          # 当前 bar 的 bar_dict（由 bar 缓存 mod 写入）
_UNIVERSE = []                    # ETF 代码列表（get_all_securities 返回）
_NAMES = {}                       # code -> display_name
_BENCHMARK = "000300.XSHG"
_LIST_DATES = {}                  # code -> (list_date, delist_date) 'YYYY-MM-DD'（get_all_securities 过滤用）
_EVERY_BAR_CALLBACKS = []         # 'every_bar' 调度回调
_DAILY_AT = {}                      # (hour, minute) -> 聚宽 run_daily(time='HH:MM') 回调
_FQ_WARNED = set()                  # 已提示 fq 口径不支持的标的（install_jqcompat 每次回测重置）


def _set_current_bar_dict(bd):
    global _CURRENT_BAR_DICT
    _CURRENT_BAR_DICT = bd


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def _norm_freq(freq):
    freq = (freq or "1d").lower()
    if freq in ("daily", "day", "1d"):
        return "1d"
    if freq in ("min", "minute", "1m"):
        return "1m"
    return freq


def _dt_to_int(dt, freq):
    ts = pd.Timestamp(dt)
    y, m, d = ts.year, ts.month, ts.day
    base = y * 10000000000 + m * 100000000 + d * 1000000
    if freq == "1m":
        return base + ts.hour * 10000 + ts.minute * 100 + ts.second
    return base


def _instrument(code):
    try:
        return Environment.get_instance().data_proxy.get_instrument(code)
    except Exception:
        return None


def _actual_fields(fields):
    """聚宽字段名 -> rqalpha recarray 字段名。"""
    out = []
    for f in fields:
        out.append("total_turnover" if f == "money" else f)
    return out


def _bars_to_long_df(bars, code, requested_fields, freq):
    dt = bars["datetime"]
    times = pd.to_datetime(dt.astype("int64").astype(str), format="%Y%m%d%H%M%S")
    rows = {"time": times, "code": code}
    for f in requested_fields:
        actual = "total_turnover" if f == "money" else f
        rows[f] = np.asarray(bars[actual]) if actual in bars.dtype.names else np.nan
    return pd.DataFrame(rows)


def _count_bars(start_dt, end_dt, freq):
    env = Environment.get_instance()
    cal = env.data_proxy.get_trading_calendar()
    s = pd.Timestamp(start_dt).normalize()
    e = pd.Timestamp(end_dt).normalize()
    n_days = int(((cal[(cal >= s) & (cal <= e)]).size) or 1)
    if freq == "1m":
        return n_days * 240
    return n_days


def _warn_fq_once(code, fq):
    """fq 口径不支持显式化：每标的每次回测只 warning 一次，不刷屏。

    现状：本地日线统一为前复权口径（上游 tushare qfq，各源 adj 元数据由
    jqengine 侧维护），fq="pre"/"qfq"/None 已直接满足；fq="none"/"raw"（不
    复权）与 "post"/"hfq"（后复权）本地无对应口径数据，实际仍返回前复权价，
    与请求口径不一致（不复权口径在除息/除权日附近收益有偏差）。
    """
    if code in _FQ_WARNED:
        return
    _FQ_WARNED.add(code)
    logger.warning(
        "%s: fq=%s 请求的口径本地不支持（本地日线统一为前复权），实际返回前复权价格；"
        "除息/除权日附近收益口径与该 fq 语义有偏差（每标的每次回测仅提示一次）",
        code, fq,
    )


def _check_fq(fq, codes):
    """fq 口径检查：本地日线统一前复权，pre/qfq/None 直接满足不提示；
    none/raw/post/hfq 等口径无法满足，逐标的 warning 一次。"""
    if fq is None or str(fq).lower() in ("pre", "qfq"):
        return
    for code in codes:
        _warn_fq_once(code, fq)


# ---------------------------------------------------------------------------
# 聚宽 API shim
# ---------------------------------------------------------------------------
def get_price(security, start_date=None, end_date=None, frequency="1d", fields=None,
               panel=True, skip_paused=True, fq="pre", count=None, **kwargs):
    """聚宽 get_price。

    fq 现状：本地日线统一为前复权口径，fq="pre"/"qfq"/None 直接满足（不提示）；
    fq="none"/"raw"/"post"/"hfq" 本地无对应口径，实际仍返回前复权价，每标的
    每次回测 logger.warning 一次（见 _warn_fq_once）。
    """
    env = Environment.get_instance()
    dp = env.data_proxy
    freq = _norm_freq(frequency)
    if isinstance(security, str):
        codes = [security]
    else:
        codes = list(security)
    _check_fq(fq, codes)
    if fields is None:
        fields = ["open", "close", "high", "low", "volume", "money"]
    else:
        fields = list(fields)
    end_dt = pd.Timestamp(end_date) if end_date is not None else env.trading_dt
    # 聚宽语义收口：'1d' 不含"未完成的当日日线"。end 落在当前交易日且当前时刻
    # <15:00 时，有效末端回退到前一交易日（数据源层对 00:00 纯日期按 rqalpha
    # 原生口径含当日，聚宽 attribute_history 恒不含当前 bar 的语义在此保证；
    # 盘中 BAR 时刻由 _daily_slice_side 排除当日，此处主要覆盖 BEFORE_TRADING
    # 等 00:00 调用）。显式传入的历史 end_date（< 今日）不受影响，含当日。
    if freq == "1d":
        _now = pd.Timestamp(getattr(env, "trading_dt", end_dt))
        if (end_dt >= _now.normalize()
                and (_now.hour, _now.minute, _now.second) < (15, 0, 0)):
            try:
                end_dt = pd.Timestamp(
                    env.data_proxy.get_previous_trading_date(_now.date()))
            except Exception:
                end_dt = _now.normalize() - pd.Timedelta(days=1)

    # 批量路径：多标的 + 已知 count（策略动量/流动性主用）一次性切片，避免逐只
    # 调用 history_bars 在 1000+ 标的上产生上万次函数与 searchsorted 开销。
    # 同时一次性向量化构造 DataFrame（仅 1 次 to_datetime / concat），消除逐只
    # _bars_to_long_df 在 2.3 万次调用上的 Python 与解析开销。
    if len(codes) > 1 and (count is not None or start_date is not None):
        try:
            if count is not None:
                _bc = int(count)
            else:
                _bc = _count_bars(start_date, end_dt, freq)
            ds = env.data_source
            bat = ds.history_bars_batch(codes, _bc, freq, _actual_fields(fields), end_dt)
            _tlist, _clist, _flist = [], [], {f: [] for f in fields}
            for code in codes:
                bars = bat.get(code)
                if bars is None or len(bars) == 0:
                    continue
                n = len(bars)
                _tlist.append(np.asarray(bars["datetime"], dtype="int64"))
                _clist.append(code)
                for f in fields:
                    actual = "total_turnover" if f == "money" else f
                    _flist[f].append(
                        np.asarray(bars[actual]) if actual in bars.dtype.names
                        else np.full(n, np.nan))
            if _tlist:
                times = np.concatenate(_tlist)
                data = {
                    "time": pd.to_datetime(times.astype(str), format="%Y%m%d%H%M%S"),
                    "code": np.repeat(np.array(_clist, dtype=object), [len(t) for t in _tlist]),
                }
                for f in fields:
                    data[f] = np.concatenate(_flist[f])
                out = pd.DataFrame(data)
                if start_date is not None:
                    sd = pd.Timestamp(start_date)
                    out = out[(out["time"] >= sd) & (out["time"] <= end_dt)]
                out = _fallback_volume_daily(out, codes, freq, fields, end_dt)
                return out
            out_empty = _fallback_volume_daily(
                pd.DataFrame(columns=["time", "code"] + fields), codes, freq, fields, end_dt)
            return out_empty
        except Exception as e:
            logger.debug("get_price 批量路径失败，回退逐只: %s", e)

    frames = []
    for code in codes:
        if _instrument(code) is None:
            continue
        try:
            if count is not None:
                bars = dp.history_bars(code, int(count), freq, _actual_fields(fields), end_dt)
            elif start_date is not None:
                n = _count_bars(start_date, end_dt, freq)
                bars = dp.history_bars(code, n, freq, _actual_fields(fields), end_dt)
            else:
                bars = dp.history_bars(code, 1, freq, _actual_fields(fields), end_dt)
        except Exception as e:
            logger.debug("get_price %s 失败: %s", code, e)
            bars = None
        if bars is None or len(bars) == 0:
            continue
        df = _bars_to_long_df(bars, code, fields, freq)
        if start_date is not None:
            sd = pd.Timestamp(start_date)
            df = df[(df["time"] >= sd) & (df["time"] <= end_dt)]
        if not df.empty:
            frames.append(df)

    if not frames:
        return _fallback_volume_daily(
            pd.DataFrame(columns=["time", "code"] + fields), codes, freq, fields, end_dt)
    out = pd.concat(frames, ignore_index=True)
    out = _fallback_volume_daily(out, codes, freq, fields, end_dt)
    return out


def _fallback_volume_daily(out, codes, freq, fields, end_dt):
    """当日 1 分钟成交量缺失时，降级用日线当日成交量（仅收盘后）。

    收盘后（end_dt >= 15:00）当日日线已完整，聚宽的当日成交量 == 日线成交量
    （分钟量之和），故等价；避免免费源历史分钟缺失导致成交量过滤把所有标的
    误杀（早期回测区间无分钟数据）。盘中（<15:00）禁用本兜底：分钟缺失时把
    日线全天量贴到盘中时刻是前视（09:40 即拿到全天量，策略量比推算 ×24 倍
    失真），此时分钟量缺失即视为不可用，由策略走既有缺失兜底。仅在请求
    字段仅为 volume 的 1m 模式下触发，不影响 OHLC 等其他分钟取数。
    """
    if freq != "1m" or fields != ["volume"]:
        return out
    # 盘中防前视：15:00 前不得贴全天成交量（见 docstring）
    _et = pd.Timestamp(end_dt)
    if (_et.hour, _et.minute, _et.second) < (15, 0, 0):
        return out
    try:
        ds = Environment.get_instance().data_source
        store = getattr(ds, "_day_bar_store", None)
        if store is None:
            return out
    except Exception:
        return out
    target = int(_et.strftime("%Y%m%d"))
    have = set(out["code"].tolist()) if not out.empty else set()
    add = []
    for code in codes:
        if code in have:
            continue
        try:
            bars = store.get_bars(code)
            if bars is None or len(bars) == 0:
                continue
            di = (bars["datetime"] // 1000000).astype("int64")
            m = di == target
            if not m.any():
                continue
            v = float(bars["volume"][m][-1])
            add.append({"time": pd.Timestamp(end_dt), "code": code, "volume": v})
        except Exception:
            continue
    if add:
        add_df = pd.DataFrame(add)
        out = pd.concat([out, add_df], ignore_index=True) if not out.empty else add_df
    return out


def _is_index_code(code):
    """指数代码判定（000xxx.XSHG / 399xxx.XSHE）。

    数据源宇宙为支持指数类查询（如策略的 000300/000510 大盘判断）会混入指数
    代码；聚宽 get_all_securities(['etf']) 只返回真实 ETF——不剔除时指数
    （其 amount 是成分股全市场成交额，万亿级）会被算进"全市场 ETF 总成交额"，
    实测本地聚合 41.7 亿中 36.4 亿来自 4 只指数（修正后 ETF 合计 5304 亿 vs
    聚宽 4967 亿）。
    """
    pure, _, exch = code.partition(".")
    return (exch == "XSHG" and pure.startswith("000")) or (
        exch == "XSHE" and pure.startswith("399"))


def get_all_securities(types=None, date=None):
    """对齐聚宽 get_all_securities：date 给定（'YYYY-MM-DD'）时按上市/退市日期过滤。

    types 过滤：指数代码只在 types 含 'index' 时返回（对齐聚宽
    get_all_securities(['etf']) 只含真实 ETF）；types 为 None 时返回全部。
    起止日期来自 install_jqcompat 注入的 _LIST_DATES（tushare fund_basic 的
    list_date/delist_date）；数据不可得的标的退化为全程可交易
    （2000-01-01 ~ 2999-12-31），保持修复前行为，避免误杀。
    """
    cutoff = str(pd.Timestamp(date).date()) if date is not None else None
    type_set = None
    if types is not None:
        type_set = {str(t).lower() for t in (types if isinstance(types, (list, tuple, set)) else [types])}
    rows = []
    for code in _UNIVERSE:
        is_idx = _is_index_code(code)
        if type_set is not None:
            if is_idx and "index" not in type_set:
                continue
            if not is_idx and type_set == {"index"}:
                continue
        start_s, end_s = _LIST_DATES.get(code, ("2000-01-01", "2999-12-31"))
        if cutoff is not None and not (start_s <= cutoff <= end_s):
            continue
        rows.append({
            "order_book_id": code,
            "display_name": _NAMES.get(code, code),
            "name": _NAMES.get(code, code),
            "start_date": start_s,
            "end_date": end_s,
            "type": "index" if is_idx else "etf",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.set_index("order_book_id")
    return df


def get_security_name(code):
    try:
        if code in _NAMES:
            return _NAMES[code]
        ins = _instrument(code)
        if ins is not None:
            return getattr(ins, "symbol", code) or code
    except Exception:
        pass
    return "未知名称"


def get_security_info(code):
    return _instrument(code)


def attribute_history(security, count, unit="1d", fields=None, skip_paused=True,
                      fq="pre", **kwargs):
    """聚宽 attribute_history：恒不含「当前 bar」。

    盘中（<15:00）取 '1d' 时，当日日线尚未完成，数据源层（JqDataSource.
    history_bars，include_now 默认 False）会裁掉 dt 当日 bar，杜绝前视；
    15:00 起当日日线已完整，含当日。

    fq 现状：本地日线统一为前复权口径，fq="pre"/"qfq"/None 直接满足；
    none/raw/post/hfq 等口径无法满足，仅显式 warning（每标的每次回测一次，
    见 _warn_fq_once），实际返回前复权价格。
    """
    if fields is None:
        fields = ["open", "close", "high", "low", "volume"]
    df = get_price(security, count=count, frequency=unit, fields=fields, panel=False, fq=fq)
    if df is None or df.empty:
        return pd.DataFrame()
    return df.set_index("time")[list(fields)]


def is_temporarily_suspended(security, context=None, minute_count=10):
    try:
        minute_data = get_price(
            security, end_date=(context.current_dt if context is not None else None),
            count=minute_count, frequency="1m", fields=["volume"], skip_paused=False, fq="pre",
        )
        if minute_data is None or minute_data.empty:
            return True
        if (minute_data["volume"] == 0).all():
            return True
        return False
    except Exception as e:
        logger.debug("临时停牌检测异常 %s: %s", security, e)
        return False


def get_trade_days(start_date=None, end_date=None, count=None):
    env = Environment.get_instance()
    cal = env.data_proxy.get_trading_calendar()
    cal = pd.DatetimeIndex(cal)
    if count is not None:
        if end_date is not None:
            end = pd.Timestamp(end_date)
            idx = int(cal.searchsorted(pd.Timestamp(end)))
            return list(cal[max(0, idx - int(count) + 1): idx + 1])
        return list(cal[: int(count)])
    if start_date is not None and end_date is not None:
        s = pd.Timestamp(start_date).normalize()
        e = pd.Timestamp(end_date).normalize()
        return [d for d in cal if s <= d.normalize() <= e]
    return list(cal)


def history(security, bar_count, unit="1d", field=None, skip_paused=True, df=True,
            fq="pre", **kwargs):
    """聚宽 history。fq 现状同 get_price：本地日线统一前复权，pre/qfq/None
    直接满足，其余口径仅 warning 一次、实际返回前复权价。"""
    fields = [field] if isinstance(field, str) else (field or ["close"])
    df_out = get_price(security, count=bar_count, frequency=unit, fields=fields, panel=False, fq=fq)
    if df_out is None or df_out.empty:
        return pd.DataFrame()
    return df_out.set_index("time")[list(fields)]


def set_benchmark(code):
    global _BENCHMARK
    _BENCHMARK = code


def set_slippage(*args, **kwargs):
    pass


def set_order_cost(*args, **kwargs):
    pass


def set_option(*args, **kwargs):
    pass


# 方案 A（current_price=昨收）已证伪，保留兼容接口
_use_prev_close = False

def set_use_prev_close(val: bool):
    global _use_prev_close
    _use_prev_close = val

def get_use_prev_close() -> bool:
    return _use_prev_close


class PriceRelatedSlippage:
    def __init__(self, *a, **k):
        pass


class OrderCost:
    def __init__(self, *a, **k):
        pass


# ---- log shim ----
# LIVE_SINK：实时写库钩子（由回测 LiveStreamMod 在运行期注入），
# 策略日志会被实时推给 SSE；不设置时退化为普通日志。
LIVE_SINK = None


class _Log:
    def _sink(self, level, a):
        msg = " ".join(str(x) for x in a)
        try:
            with open("/tmp/wufu_dbg.log", "a", encoding="utf-8") as _f:
                _f.write(f"[{level}] {msg}\n")
                _f.flush()
        except Exception:
            pass
        if LIVE_SINK is not None:
            try:
                LIVE_SINK(level, msg)
            except Exception:
                pass

    def info(self, *a, **k):
        self._sink("INFO", a)
        logger.info(*a)

    def warn(self, *a, **k):
        self._sink("WARNING", a)
        print("[策略WARN]", *a)
        logger.warning(*a)

    warning = warn

    def error(self, *a, **k):
        self._sink("ERROR", a)
        print("[策略ERROR]", *a)
        logger.error(*a)

    def debug(self, *a, **k):
        self._sink("DEBUG", a)
        print("[策略DEBUG]", *a)
        logger.debug(*a)

    def set_level(self, *a, **k):
        pass


def record(*args, **kwargs):
    # 聚宽 record 用于绘制时间序列；此处仅作 no-op（净值/成交由 analyser 回收）
    return None


def run_minute(func, second=0, **kwargs):
    logger.warning("run_minute 未完整实现（本策略未使用），已忽略: %s", getattr(func, "__name__", func))
    return None

def run_daily(func, time=None, *args, **kwargs):
    """兼容聚宽 run_daily：支持 time='HH:MM' / 'before_trading' / 'every_bar'。

    聚宽回调签名是 ``func(context)``，而 rqalpha 调度器要求
    ``func(context, bar_dict)``，这里用包装函数做桥接。

    全局作用域调用时（聚宽允许，rqalpha 不允许），缓存到 _PENDING_RUN_DAILY，
    由 mod start_up 时 _replay_run_daily 重放。
    """
    time_rule = time if time is not None else kwargs.get("time_rule")
    if time_rule is None:
        time_rule = "before_trading"

    def _wrapper(context, bar_dict=None):
        return func(context)

    if time_rule == "every_bar":
        _EVERY_BAR_CALLBACKS.append(func)
        return None

    # 把各语义时间归一为具体 (hour, minute)，注册到分钟级 BAR 事件，
    # 由 bar 缓存 mod 的 _on_bar 在对应分钟触发（绕过 rqalpha scheduler
    # 的 ON_INIT 阶段限制，且不受全局调用时机影响）。
    if time_rule == "before_trading":
        hm = (9, 31)
    elif time_rule == "open":
        hm = (9, 32)
    elif time_rule == "close":
        hm = (15, 0)
    elif isinstance(time_rule, int):
        hm = divmod(max(0, min(time_rule, 15 * 60)), 60)
    else:
        try:
            hh, mm = str(time_rule).split(":")
            hm = (int(hh), int(mm))
        except Exception:
            hm = (9, 31)
    # 交易时段外的时刻归并到最近的分钟 bar 时刻：分钟 bar 只覆盖 09:31~15:00，
    # 调度按 (hour,minute) 精确匹配 bar，时段外时刻（如聚宽晨间流水线 '09:00'、
    # 盘后 '15:10'）永不触发。盘前(≤09:30)→首根 bar 09:31，盘后(≥15:00)→末根
    # bar 15:00。
    if hm <= (9, 30):
        hm = (9, 31)
    elif hm >= (15, 0):
        hm = (15, 0)
    _DAILY_AT.setdefault(hm, []).append(func)
    return None


def _replay_run_daily():
    """兼容层保留接口（聚宽全局 run_daily 已在 exec 时直接注册到分钟事件，
    不再需要 init 内重放）。保留为空操作以避免历史调用报错。"""
    return None


# ---------------------------------------------------------------------------
# get_current_data
# ---------------------------------------------------------------------------
class _SecurityData:
    def __init__(self, code):
        self.code = code

    @property
    def _env(self):
        return Environment.get_instance()

    @property
    def _bar(self):
        bd = _CURRENT_BAR_DICT
        if bd is not None and self.code in bd:
            return bd[self.code]
        return None

    def _fallback_price(self):
        instr = _instrument(self.code)
        if instr is None:
            return 0.0
        for freq in ("1m", "1d"):
            try:
                # rqalpha 6.x DataProxy.get_bar 第一个参数是 order_book_id 字符串，
                # 传 Instrument 对象会在 instruments() 里 TypeError 被静默吞掉；
                # 返回值是 BarObject，__getitem__ 读实例 __dict__（cached_property
                # 未访问前不存在），b["close"] 必抛 KeyError——两处问题叠加曾导致
                # 未订阅标的 last_price 恒为 0（对账实测 589720 动量分被 price=0
                # 拉爆、跌出得分表 top-100，且 low_limit=0 触发"跌停跳过买入"）。
                b = self._env.data_proxy.get_bar(self.code, self._env.trading_dt, freq)
                if b is not None and b.close == b.close:
                    return float(b.close)
            except Exception:
                continue
        return 0.0

    @property
    def last_price(self):
        bar = self._bar
        if bar is not None and bar.last == bar.last:
            return float(bar.last)
        return self._fallback_price()

    @property
    def price(self):
        return self.last_price

    @property
    def high_limit(self):
        bar = self._bar
        if bar is not None and bar.limit_up == bar.limit_up:
            return float(bar.limit_up)
        instr = _instrument(self.code)
        if instr is not None:
            try:
                # 同 _fallback_price：get_bar 传 order_book_id 字符串、
                # BarObject 用属性访问（b.limit_up 而非 b["limit_up"]）
                b = self._env.data_proxy.get_bar(self.code, self._env.trading_dt, "1d")
                if b is not None:
                    return float(b.limit_up)
            except Exception:
                pass
        return 0.0

    @property
    def low_limit(self):
        bar = self._bar
        if bar is not None and bar.limit_down == bar.limit_down:
            return float(bar.limit_down)
        instr = _instrument(self.code)
        if instr is not None:
            try:
                # 同 _fallback_price：get_bar 传 order_book_id 字符串、
                # BarObject 用属性访问（b.limit_down 而非 b["limit_down"]）
                b = self._env.data_proxy.get_bar(self.code, self._env.trading_dt, "1d")
                if b is not None:
                    return float(b.limit_down)
            except Exception:
                pass
        return 0.0

    @property
    def paused(self):
        return False

    @property
    def is_trading(self):
        return True

    @property
    def suspended(self):
        return False


class _CurrentData(dict):
    def __getitem__(self, code):
        return _SecurityData(code)

    def __contains__(self, code):
        return True


def get_current_data():
    return _CurrentData()


# ---------------------------------------------------------------------------
# 数据源：日线 + 合成分钟线（基于 manager.QuantDataProvider）
# ---------------------------------------------------------------------------
_BAR_DTYPE = np.dtype([
    ("datetime", np.int64),
    ("open", np.float64),
    ("close", np.float64),
    ("high", np.float64),
    ("low", np.float64),
    ("volume", np.float64),
    ("total_turnover", np.float64),
    ("limit_up", np.float64),
    ("limit_down", np.float64),
])


class _DayBarStore:
    """惰性加载日线：首次请求时从 DataManager 取数并缓存，避免构造期遍历 1600+ 标的。"""
    def __init__(self, dm, data_start, data_end):
        self._dm = dm
        self._data_start = data_start
        self._data_end = data_end
        self._bars = {}

    def get_bars(self, order_book_id):
        if order_book_id in self._bars:
            return self._bars[order_book_id]
        # 惰性加载
        try:
            ddf = self._dm.fetch("get_daily", order_book_id,
                                 self._data_start.strftime("%Y%m%d"),
                                 self._data_end.strftime("%Y%m%d"))
        except Exception:
            ddf = None
        if ddf is None or len(ddf) == 0:
            arr = np.empty(0, dtype=_BAR_DTYPE)
        else:
            ddf = _normalize_daily(ddf)
            ddf = ddf[(ddf["date"] >= self._data_start) & (ddf["date"] <= self._data_end)]
            if ddf.empty:
                arr = np.empty(0, dtype=_BAR_DTYPE)
            else:
                arr = _daily_to_recarray(ddf, code=order_book_id)
        self._bars[order_book_id] = arr
        return arr

    def preload(self, all_daily, codes, data_start=None, data_end=None):
        """一次性把内存缓存中的日线铺进 _bars，避免回测中逐标的 fetch（SQLite）。

        all_daily: cache.get_all("daily") 返回的 {key: DataFrame}（已全在内存）。
        codes: 标的 order_book_id 列表（形如 512670.XSHG）。缓存 key 为
        'astock_<code>'，回退到 '<code>'；命中即转 recarray 直接写入 _bars。
        """
        ds = data_start if data_start is not None else self._data_start
        de = data_end if data_end is not None else self._data_end
        for code in codes:
            if code in self._bars:
                continue
            df = all_daily.get("astock_" + code)
            if df is None:
                df = all_daily.get("tushare_" + code)
            if df is None:
                df = all_daily.get("baostock_" + code)
            if df is None:
                df = all_daily.get(code)
            if df is None or getattr(df, "empty", True):
                self._bars[code] = np.empty(0, dtype=_BAR_DTYPE)
                continue
            ddf = _normalize_daily(df)
            ddf = ddf[(ddf["date"] >= ds) & (ddf["date"] <= de)]
            if ddf.empty:
                arr = np.empty(0, dtype=_BAR_DTYPE)
            else:
                arr = _daily_to_recarray(ddf, code=code)
            self._bars[code] = arr

    def get_date_range(self, order_book_id):
        bars = self.get_bars(order_book_id)
        if len(bars) == 0:
            return 20050104, 20050104
        return int(bars["datetime"][0]), int(bars["datetime"][-1])


class _CalendarStore:
    def __init__(self, dates):
        self._calendar = pd.DatetimeIndex(sorted(dates))

    def get_trading_calendar(self):
        return self._calendar


def _normalize_daily(df):
    if not isinstance(df.index, pd.DatetimeIndex):
        for c in ("datetime", "date", "trade_date", "time"):
            if c in df.columns:
                df = df.set_index(pd.to_datetime(df[c], errors="coerce"))
                break
    df = df[~df.index.isna()].sort_index()
    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    if "volume" in df.columns:
        vol = df["volume"].astype(float)
    elif "vol" in df.columns:
        vol = df["vol"].astype(float) * 100.0
    else:
        vol = c * 0.0
    if "money" in df.columns:
        # 优先 money：DataManager 侧已按源归一到元（tushare amount 为千元，
        # 见 manager._ensure_money_yuan）；amount 仅作无 money 时的兜底
        turn = df["money"].astype(float)
    elif "amount" in df.columns:
        turn = df["amount"].astype(float)
    else:
        turn = c * vol
    out = pd.DataFrame({
        "date": df.index,
        "open": o, "high": h, "low": l, "close": c,
        "volume": vol, "total_turnover": turn,
    })
    return out


def _is_st_name(code, name=None):
    """按名称判定 ST 股票：名称含 "ST"（不区分大小写，覆盖 *ST/ST）判 True。

    name 缺省时取 _NAMES 名称映射；取不到名称时按非 ST 处理（不制造虚假
    ST 标记，涨跌停幅度也不会被错压到 5%）。
    """
    if name is None:
        name = _NAMES.get(code)
    return bool(name) and "ST" in str(name).upper()


def _limit_rate(code, name=None):
    """按代码分档的涨跌停幅度（供日线/分钟 recarray 与 rqalpha_bridge 复用）。

    - 沪市 68xxxx（科创板股票）、58xxxx（科创板 ETF/LOF）：±20%
    - 深市 30xxxx（创业板股票）、159xxx（创业板 ETF）：±20%
    - ST 股票（名称含 "ST"，取不到名称按非 ST）：±5%
    - 其余主板股票/ETF：±10%
    北交所 8xxxxx/4xxxxx 实为 ±30%，但本代码体系（聚宽 XSHG/XSHE 后缀）
    覆盖不到北交所标的，不做分档，随主板 ±10%。
    """
    pure, _, exch = code.partition(".")
    if exch in ("", "XSHG") and pure.startswith(("68", "58")):
        return 0.20
    if exch in ("", "XSHE") and pure.startswith(("30", "159")):
        return 0.20
    if _is_st_name(code, name):
        return 0.05
    return 0.10


def _limit_prices_from_prev_close(close: "pd.Series", rate: float = 0.10):
    """按昨收计算涨跌停价：limit = round(prev_close × (1±rate), 2)。

    交易所口径以昨收为基准（此前用当根 high×1.1/low×0.9，导致
    last_price >= high_limit 恒 False）。首日无前收 → NaN：不用当日 open
    估算，避免制造虚假涨跌停触发；NaN 在策略比较中恒 False，行为可预期。
    rate 按标的分档（见 _limit_rate）：科创/创业 20cm、ST 5%、主板 10%；
    调用方未给标的时保持主板 10% 兜底。
    """
    prev_close = close.shift(1)
    limit_up = (prev_close * (1 + rate)).round(2)
    limit_down = (prev_close * (1 - rate)).round(2)
    return limit_up.to_numpy(dtype=np.float64), limit_down.to_numpy(dtype=np.float64)


def _daily_to_recarray(df, code=None):
    df = _normalize_daily(df)
    n = len(df)
    if n == 0:
        return np.empty(0, dtype=_BAR_DTYPE)
    arr = np.zeros(n, dtype=_BAR_DTYPE)
    _idx = pd.DatetimeIndex(df["date"])
    arr["datetime"] = (
        _idx.year.astype("int64") * 10000
        + _idx.month.astype("int64") * 100
        + _idx.day.astype("int64")
    ) * 1000000
    arr["open"] = df["open"].to_numpy(dtype=np.float64)
    arr["close"] = df["close"].to_numpy(dtype=np.float64)
    arr["high"] = df["high"].to_numpy(dtype=np.float64)
    arr["low"] = df["low"].to_numpy(dtype=np.float64)
    arr["volume"] = df["volume"].to_numpy(dtype=np.float64)
    arr["total_turnover"] = df["total_turnover"].to_numpy(dtype=np.float64)
    arr["limit_up"], arr["limit_down"] = _limit_prices_from_prev_close(
        df["close"], rate=_limit_rate(code) if code else 0.10)
    return arr


def _minute_to_recarray(minute_df, prev_close_map=None, code=None):
    """分钟 DataFrame -> bar recarray。

    prev_close_map: {日期 int(YYYYMMDD): 昨收}，用于按昨收计算当日涨跌停价
    （口径同 _limit_prices_from_prev_close，幅度按 code 分档见 _limit_rate）；
    缺昨收的交易日涨跌停为 NaN。
    """
    if minute_df is None or len(minute_df) == 0:
        return np.empty(0, dtype=_BAR_DTYPE)
    df = minute_df.sort_index()
    n = len(df)
    arr = np.zeros(n, dtype=_BAR_DTYPE)
    _idx = pd.DatetimeIndex(df.index)
    arr["datetime"] = (
        _idx.year.astype("int64") * 10000000000
        + _idx.month.astype("int64") * 100000000
        + _idx.day.astype("int64") * 1000000
        + _idx.hour.astype("int64") * 10000
        + _idx.minute.astype("int64") * 100
        + _idx.second.astype("int64")
    ).to_numpy(dtype=np.uint64)
    arr["open"] = df["open"].to_numpy(dtype=np.float64)
    arr["close"] = df["close"].to_numpy(dtype=np.float64)
    arr["high"] = df["high"].to_numpy(dtype=np.float64)
    arr["low"] = df["low"].to_numpy(dtype=np.float64)
    arr["volume"] = df["volume"].to_numpy(dtype=np.float64)
    # 成交额优先使用真实 money/amount（real 1m / baostock 插值均带 money），否则用 close*volume
    if "money" in df.columns and df["money"].notna().any():
        turn = df["money"].astype(float).to_numpy(dtype=np.float64)
    elif "amount" in df.columns and df["amount"].notna().any():
        turn = df["amount"].astype(float).to_numpy(dtype=np.float64)
    else:
        turn = (df["close"] * df["volume"]).to_numpy(dtype=np.float64)
    arr["total_turnover"] = turn
    _date_ints = _idx.year * 10000 + _idx.month * 100 + _idx.day
    if prev_close_map:
        _prev = pd.to_numeric(
            pd.Index(_date_ints).map(prev_close_map), errors="coerce"
        ).to_numpy(dtype=np.float64)
    else:
        _prev = np.full(n, np.nan)
    _rate = _limit_rate(code) if code else 0.10
    arr["limit_up"] = np.round(_prev * (1 + _rate), 2)
    arr["limit_down"] = np.round(_prev * (1 - _rate), 2)
    return arr



class _MinuteBarStore:
    """包装分钟线 dict，提供 get_bars 接口。"""
    def __init__(self, bars):
        self._bars = bars

    def get_bars(self, order_book_id):
        return self._bars.get(order_book_id, np.empty(0, dtype=_BAR_DTYPE))


def _fund_instrument_type(code):
    """按代码段判定基金/证券类型（与 tushare get_etf_list 的 LOF 前缀口径一致）。

    上交所(XSHG)：51/56/58 → ETF，50 → LOF；深交所(XSHE)：15 → ETF，16 → LOF；
    其余 → CS。rqalpha 股票账户支持 ETF/LOF 交易（INST_TYPE_IN_STOCK_ACCOUNT），
    且 sys_transaction_cost 只对 CS 收卖出印花税 —— ETF/LOF 现实免税，
    类型必须标对，否则回测多扣 0.05% 印花税。
    """
    pure, _, exch = code.partition(".")
    if exch == "XSHG":
        if pure.startswith(("51", "56", "58")):
            return "ETF"
        if pure.startswith("50"):
            return "LOF"
    elif exch == "XSHE":
        if pure.startswith("15"):
            return "ETF"
        if pure.startswith("16"):
            return "LOF"
    return "CS"


def _daily_slice_side(dt, include_now):
    """'1d' 取数的 searchsorted 侧别（盘中防前视，对齐聚宽/rqalpha 原生语义）。

    dt 处于盘中时刻（(00:00, 15:00) 开区间）且 include_now=False 时，当日日线
    尚未完成，不含 dt 当日 bar（side='left'）。dt 为纯日期（00:00）时按 rqalpha
    原生口径含当日（side='right'）——sys_analyser 等 rqalpha 内部调用以
    dt=末日日期取基准日线，排除当日会让基准序列缺末日 bar 直接报错；聚宽
    「不含当前 bar」的策略侧语义由 get_price shim 在调用前回退 end_dt 收口。
    15:00 起当日日线已完整，含当日：日线频率回测 handle_bar 于 15:00 触发。
    """
    if include_now:
        return "right"
    t = pd.Timestamp(dt)
    return "left" if (0, 0, 0) < (t.hour, t.minute, t.second) < (15, 0, 0) else "right"


class JqDataSource:
    """基于 DataManager（原始缓存数据：real 1m + baostock 5min 插值 + 日线合成兜底）
    的 rqalpha 数据源。

    日线在构造时一次性展开（覆盖全 ETF 宇宙，内存占用小）；分钟线**惰性**加载并按
    LRU 上限回收，避免为 1600+ 标的预生成全段 1m 历史导致 OOM。兼容 1m 频率回测。
    """

    def __init__(self, dm, universe, start, end, benchmark=None, lookback_days=250,
                 minute_cache_cap=800):
        self._dm = dm
        self._universe = list(universe)
        self._start = pd.Timestamp(start).normalize()
        self._end = pd.Timestamp(end).normalize()
        # 日线数据加载末端须严格超出回测末端：rqalpha 对基准/数据范围做严格校验
        # （要求 data_end > backtest_end），否则报「基准数据结束日期 <= 回测结束日期」。
        # 多取的这段（非交易日）不会被回放成交，仅用于满足范围校验。
        self._data_end = self._end + pd.Timedelta(days=90)
        # 日线多取一段回看窗口：聚宽分析器需要 start-1 的基准价，策略动量需要 ~65 日回看
        self._data_start = self._start - pd.Timedelta(days=lookback_days)

        # 惰性日线 store：首次请求时从 DataManager 加载并缓存
        self._day_bar_store = _DayBarStore(dm, self._data_start, self._data_end)
        self._prev_close = {}            # code -> {date_int(YYYYMMDD): 前收盘价}，供分钟涨跌停价
        self._minute_bars = {}            # 惰性构建的分钟 recarray 缓存（与 store 共享 dict）
        self._minute_lru = []            # 访问顺序，用于 LRU 回收
        self._minute_cache_cap = minute_cache_cap
        self._instruments = {}
        all_dates = set()

        # 全量日线内存缓存（一次性取出，供日历并集与日线预加载复用，避免回测中
        # 逐标的 fetch 触发 SQLite 查询；聚宽同类数据常驻内存，这是本地慢的主因）。
        try:
            _all_daily = dm.cache.get_all("daily")
        except Exception:
            _all_daily = {}

        # 交易日历来源：直接用缓存里所有日线数据的日期并集（不依赖单个标的
        # fetch，避免 offline 模式下缓存未命中即 raise 导致日历为空、回测报
        # "区间内无数据"）。
        for _k, _df in _all_daily.items():
            if _df is None or getattr(_df, "empty", True):
                continue
            _col = "date" if "date" in _df.columns else (
                "trade_date" if "trade_date" in _df.columns else None)
            if _col is None:
                continue
            for _d in _df[_col]:
                all_dates.add(pd.Timestamp(_d).date())

        # 日线预加载：把内存缓存直接铺进 _DayBarStore._bars，回测期间 get_price
        # / history 全部命中内存，零 SQLite、零重复 _normalize_daily。
        _preload_codes = list(self._universe)
        if benchmark:
            _preload_codes.append(benchmark)
        self._day_bar_store.preload(_all_daily, _preload_codes,
                                    self._data_start, self._data_end)

        # 预加载基准日线（rqalpha 校验基准数据范围）
        if benchmark:
            try:
                bdf = dm.fetch("get_daily", benchmark,
                               self._data_start.strftime("%Y%m%d"),
                               self._data_end.strftime("%Y%m%d"))
                if bdf is not None and len(bdf):
                    bdf = _normalize_daily(bdf)
                    bdf = bdf[(bdf["date"] >= self._data_start) & (bdf["date"] <= self._data_end)]
                    if not bdf.empty:
                        self._day_bar_store._bars[benchmark] = _daily_to_recarray(bdf, code=benchmark)
                        if "pre_close" in bdf.columns:
                            _dints = (
                                pd.DatetimeIndex(bdf["date"]).year * 10000
                                + pd.DatetimeIndex(bdf["date"]).month * 100
                                + pd.DatetimeIndex(bdf["date"]).day
                            ).to_numpy(dtype=np.int64)
                            self._prev_close[benchmark] = dict(
                                zip(_dints.tolist(), bdf["pre_close"].astype(float).to_numpy().tolist())
                            )
                        for d in bdf["date"]:
                            all_dates.add(pd.Timestamp(d).date())
                        # 创建基准 instrument
                        self._instruments[benchmark] = self._make_instrument(benchmark)
            except Exception:
                pass

        self._trading_dates = sorted(all_dates)

        self._day_bar_stores = {}
        self._minute_bar_stores = {}
        self._calendar_stores = {}
        # 基金标的（ETF/LOF，见 _fund_instrument_type）与 CS 共用同一套 bar store
        for _t in (INSTRUMENT_TYPE.CS, INSTRUMENT_TYPE.ETF, INSTRUMENT_TYPE.LOF):
            self.register_day_bar_store(_t, self._day_bar_store, market=MARKET.CN)
            self.register_minute_bar_store(_t, _MinuteBarStore(self._minute_bars), market=MARKET.CN)
        self.register_calendar_store(TRADING_CALENDAR_TYPE.CN_STOCK, _CalendarStore(self._trading_dates))

    # ---- 惰性分钟线（LRU 回收） ----
    def _prev_close_map(self, code):
        """返回 {日期 int(YYYYMMDD): 昨收}，供分钟线按昨收计算涨跌停价。

        由日线 close 前移一根构建；已加载区间的首个交易日无前收（缺省，
        对应分钟 bar 涨跌停为 NaN）。构建结果缓存进 self._prev_close 复用。
        """
        m = self._prev_close.get(code)
        if m is None:
            m = {}
            bars = self._day_bar_store.get_bars(code)
            if bars is not None and len(bars) > 1:
                days = bars["datetime"] // 1000000
                closes = bars["close"]
                m = {int(days[i]): float(closes[i - 1]) for i in range(1, len(days))}
            self._prev_close[code] = m
        return m

    def _ensure_minute(self, code):
        arr = self._minute_bars.get(code)
        if arr is not None:
            return arr
        try:
            mdf = self._dm.get_minute_feed(code, self._start, self._end)
        except Exception as e:
            logger.warning("分钟线加载失败 %s: %s", code, e)
            mdf = None
        if mdf is None or len(mdf) == 0:
            return np.empty(0, dtype=_BAR_DTYPE)
        arr = _minute_to_recarray(mdf, prev_close_map=self._prev_close_map(code), code=code)
        self._minute_bars[code] = arr
        self._minute_lru.append(code)
        if len(self._minute_bars) > self._minute_cache_cap:
            old = self._minute_lru.pop(0)
            self._minute_bars.pop(old, None)
        return arr

    # ---- stores ----
    def register_day_bar_store(self, instrument_type, store, market=MARKET.CN):
        self._day_bar_stores[(instrument_type, market)] = store

    def register_minute_bar_store(self, instrument_type, store, market=MARKET.CN):
        self._minute_bar_stores[(instrument_type, market)] = store

    def register_calendar_store(self, calendar_type, store):
        self._calendar_stores[calendar_type] = store

    # ---- instrument ----
    def _make_instrument(self, code):
        return Instrument({
            "order_book_id": code,
            "symbol": _NAMES.get(code, code),
            "type": _fund_instrument_type(code),
            "round_lot": 100,
            "board_type": "MAIN",
            "exchange": code.split(".")[1] if "." in code else "XSHG",
            "listed_date": "2000-01-01",
            "de_listed_date": "2999-12-31",
            "status": "Active",
            "special_type": None,
            "market_tplus": 1,
        }, market=MARKET.CN)

    def get_instrument(self, order_book_id):
        ins = self._instruments.get(order_book_id)
        if ins is None:
            ins = self._make_instrument(order_book_id)
            self._instruments[order_book_id] = ins
        return ins

    def get_instrument_history(self, order_book_id, dt):
        return [self.get_instrument(order_book_id)]

    def get_instruments(self, id_or_syms=None, types=None):
        if id_or_syms is not None:
            for i in id_or_syms:
                ins = self._instruments.get(i)
                if ins is not None:
                    yield ins
                else:
                    yield self._make_instrument(i)
        else:
            # rqalpha 会调用此方法（无参）来构建内部标的注册表，必须返回完整 universe
            all_codes = set(self._universe)
            all_codes.update(self._instruments.keys())
            for code in all_codes:
                yield self.get_instrument(code)

    # ---- calendar ----
    def get_trading_calendars(self):
        return {t: s.get_trading_calendar() for t, s in self._calendar_stores.items()}

    def get_trading_calendar(self):
        for s in self._calendar_stores.values():
            return s.get_trading_calendar()
        return pd.DatetimeIndex([])

    def available_data_range(self, frequency):
        if not self._trading_dates:
            import datetime as _dt
            return _dt.date.min, _dt.date.max
        return self._trading_dates[0], self._trading_dates[-1]

    # ---- bars ----
    def _all_bars_of(self, instrument, freq):
        if freq == "1m":
            return self._ensure_minute(instrument.order_book_id)
        return self._day_bar_stores[(instrument.type, instrument.market)].get_bars(instrument.order_book_id)

    def history_bars(self, instrument, bar_count, frequency, fields, dt, **kwargs):
        freq = _norm_freq(frequency)
        bars = self._all_bars_of(instrument, freq)
        if len(bars) <= 0:
            return bars
        # '1d' 盘中（<15:00）且 include_now=False 时不含当日 bar（防前视，见
        # _daily_slice_side）；分钟线保持含当前 bar（rqalpha 原生语义）。
        side = _daily_slice_side(dt, kwargs.get("include_now", False)) if freq == "1d" else "right"
        i = bars["datetime"].searchsorted(
            np.uint64(_dt_to_int(dt, freq)), side=side)
        if bar_count is None:
            left = 0
        else:
            left = i - bar_count if i >= bar_count else 0
        bars = bars[left:i]
        if fields is None:
            return bars
        if isinstance(fields, str):
            return bars[["datetime", fields] if fields != "datetime" else ["datetime"]]
        wanted = ["datetime"] + [f for f in fields if f != "datetime" and f in bars.dtype.names]
        return bars[wanted]

    def history_bars_batch(self, codes, bar_count, frequency, fields, dt, include_now=False):
        """批量取多标的切片（避免 get_price 逐只调用 history_bars 的上万次开销）。

        返回 {code: bars}；bars 已按 fields 选取，与 history_bars 单只返回一致。
        '1d' 切片侧别与 history_bars 同口径（盘中防前视）。
        """
        freq = _norm_freq(frequency)
        dt_int = np.uint64(_dt_to_int(dt, freq))
        side = _daily_slice_side(dt, include_now) if freq == "1d" else "right"
        out = {}
        for code in codes:
            ins = _instrument(code)
            if ins is None:
                out[code] = None
                continue
            try:
                bars = self._all_bars_of(ins, freq)
            except Exception:
                bars = None
            if bars is None or len(bars) <= 0:
                out[code] = None
                continue
            i = bars["datetime"].searchsorted(dt_int, side=side)
            left = i - bar_count if i >= bar_count else 0
            bars = bars[left:i]
            if fields is not None:
                if isinstance(fields, str):
                    bars = bars[["datetime", fields] if fields != "datetime" else ["datetime"]]
                else:
                    wanted = ["datetime"] + [f for f in fields if f != "datetime" and f in bars.dtype.names]
                    bars = bars[wanted]
            out[code] = bars
        return out

    def get_bar(self, instrument, dt, frequency="1d"):
        freq = _norm_freq(frequency)
        bars = self._all_bars_of(instrument, freq)
        if len(bars) <= 0:
            return None
        dt_int = np.uint64(_dt_to_int(dt, freq))
        pos = bars["datetime"].searchsorted(dt_int)
        if pos >= len(bars) or bars["datetime"][pos] != dt_int:
            return None
        return bars[pos]

    def current_snapshot(self, instrument, frequency, dt):
        from rqalpha.data.data_proxy import TickObject
        bar = self.get_bar(instrument, dt, frequency)
        if bar is None:
            return None
        # prev_close 取真实昨收（无昨收数据时退化为当日 close，保持原行为）
        _pc = self._prev_close_map(instrument.order_book_id).get(
            int(bar["datetime"]) // 1000000, float(bar["close"]))
        d = {
            "datetime": int(bar["datetime"]),
            "open": float(bar["open"]), "high": float(bar["high"]),
            "low": float(bar["low"]), "last": float(bar["close"]),
            "volume": float(bar["volume"]), "total_turnover": float(bar["total_turnover"]),
            "prev_close": float(_pc),
            "limit_up": float(bar["limit_up"]), "limit_down": float(bar["limit_down"]),
        }
        return TickObject(instrument, d)

    def get_settle_price(self, instrument, date):
        bar = self.get_bar(instrument, date, "1d")
        if bar is None:
            return float("nan")
        return float(bar["close"])

    def get_open_auction_bar(self, instrument, dt):
        return self.get_bar(instrument, dt, "1d")

    # ---- 杂项（DataProxy 可能调用；无数据的返回安全值） ----
    def is_suspended(self, order_book_id, dates):
        """停牌判定：该交易日日线 bar 缺失或 volume==0 判 True。

        分钟缓存已加载时叠加分钟判据：当天无任何分钟 bar 也视为停牌（分钟线
        为惰性加载，不为停牌检查主动触发，避免全宇宙标的的分钟线 IO 风暴；
        未加载时仅按日线判定）。
        """
        bars = self._day_bar_store.get_bars(order_book_id)
        vol_by_day = {}
        if bars is not None and len(bars):
            for _dt_int, _vol in zip(bars["datetime"], bars["volume"]):
                vol_by_day[int(_dt_int) // 1000000] = float(_vol)
        minute = self._minute_bars.get(order_book_id)
        minute_days = None
        if minute is not None and len(minute):
            minute_days = set((minute["datetime"] // 1000000).astype("int64").tolist())
        out = []
        for d in dates:
            day = int(pd.Timestamp(d).strftime("%Y%m%d"))
            vol = vol_by_day.get(day)
            if vol is None or vol == 0.0:
                out.append(True)
            elif minute_days is not None and day not in minute_days:
                out.append(True)
            else:
                out.append(False)
        return out

    def is_st_stock(self, order_book_id, dates):
        """ST 判定：按名称含 "ST"（见 _is_st_name，名称取 _NAMES 映射）；
        取不到名称时按非 ST。"""
        return [_is_st_name(order_book_id)] * len(dates)

    def get_yield_curve(self, start_date, end_date, tenor=None):
        return None

    def get_dividend(self, instrument):
        return None

    def get_split(self, instrument):
        return None

    def get_ex_cum_factor(self, instrument):
        return None

    def get_exchange_rate(self, trading_date, local, settlement=MARKET.CN):
        from rqalpha.interface import ExchangeRate
        return ExchangeRate(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

    def get_open_auction_volume(self, instrument, dt):
        return 0

    def get_merge_ticks(self, order_book_id_list, trading_date, last_dt=None):
        raise NotImplementedError

    def history_ticks(self, instrument, count, dt):
        raise NotImplementedError

    def get_algo_bar(self, id_or_ins, start_min, end_min, dt):
        raise NotImplementedError

    def get_trading_minutes_for(self, instrument, trading_dt):
        raise NotImplementedError

    def append_suspend_date_set(self, date_set):
        pass

    def get_share_transformation(self, order_book_id):
        return None


# ---------------------------------------------------------------------------
# context/positions 代理：模拟聚宽持仓语义
# ---------------------------------------------------------------------------
# 聚宽 context.portfolio.positions 仅含 total_amount>0 的持仓；rqalpha 卖出后
# 仍保留 total_amount=0 的项。策略按 positions.keys() 判断持仓会导致"卖出后误判
# 仍持仓"从而同日不买入。这里包装 positions 过滤掉 0 持仓项，对齐聚宽语义。
class _PositionsView:
    def __init__(self, real):
        self._real = real

    def _active(self):
        out = {}
        try:
            items = self._real.items()
        except Exception:
            items = [(k, self._real[k]) for k in self._real.keys()]
        for k, pos in items:
            try:
                if getattr(pos, "total_amount", 0) and pos.total_amount > 0:
                    out[k] = pos
            except Exception:
                out[k] = pos
        return out

    def keys(self):
        return self._active().keys()

    def values(self):
        return self._active().values()

    def items(self):
        return self._active().items()

    def __iter__(self):
        return iter(self._active().keys())

    def __len__(self):
        return len(self._active())

    def __contains__(self, key):
        return key in self._active()

    def __getitem__(self, key):
        return self._active()[key]

    def get(self, key, default=None):
        return self._active().get(key, default)


class _PortfolioView:
    def __init__(self, real):
        self._real = real
        self._pos_view = _PositionsView(getattr(real, "positions", None))

    def __getattr__(self, name):
        if name == "positions":
            return self._pos_view
        return getattr(self._real, name)


class _ContextProxy:
    def __init__(self, real):
        self._real = real
        self._pf_view = _PortfolioView(getattr(real, "portfolio", None))

    def __getattr__(self, name):
        if name == "portfolio":
            return self._pf_view
        return getattr(self._real, name)


# ---------------------------------------------------------------------------
# bar 缓存 + every_bar 调度 mod
# ---------------------------------------------------------------------------
def _install_barcache_mod():
    if "rqalpha_mod_jqbarcache" in sys.modules:
        return
    mod = types.ModuleType("rqalpha_mod_jqbarcache")

    def load_mod():
        class _JqBarCacheMod(AbstractMod):
            def start_up(self, env, mod_config):
                # 日线频率（1d）下每天只有一根 BAR（15:00），按 (hour,minute)
                # 匹配 bar 的分钟级调度无法覆盖盘前/盘中时刻。改为：
                #   * 盘前桶 (9,31)（before_trading 及盘前时刻归并桶）在
                #     BEFORE_TRADING 触发 —— 先于当日 handle_bar；
                #   * 其余时刻（含 close/盘后归并桶 (15,0)）在 AFTER_TRADING
                #     触发 —— 晚于当日 handle_bar。
                # 分钟频率保持按 bar 时刻精确触发。
                _base_cfg = getattr(getattr(env, "config", None), "base", None)
                _freq = str(getattr(_base_cfg, "frequency", "1m") or "1m")
                self._is_daily = _freq == "1d"

                def _make_uctx():
                    _real_ctx = getattr(getattr(env, "user_strategy", None), "user_context", None)
                    return _ContextProxy(_real_ctx) if _real_ctx is not None else None

                def _fire_daily_at(hm_key=None, exclude=None):
                    uctx = _make_uctx()
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
                    _set_current_bar_dict(getattr(event, "bar_dict", None))
                    uctx = _make_uctx()
                    dt = getattr(env, "trading_dt", None)
                    hm = (dt.hour, dt.minute) if dt is not None else None
                    for cb in list(_EVERY_BAR_CALLBACKS):
                        try:
                            cb(uctx)
                        except Exception as e:
                            logger.debug("every_bar 回调异常: %s", e)
                    # 分钟频率：按 bar 的 (hour,minute) 精确触发；日线频率的
                    # run_daily 改由 BEFORE/AFTER_TRADING 分发（见上方注释），
                    # 避免 15:00 的 BAR 把盘后回调提前到 handle_bar 之前。
                    if hm is not None and not self._is_daily:
                        for cb in list(_DAILY_AT.get(hm, [])):
                            try:
                                cb(uctx)
                            except Exception as e:
                                logger.warning("daily_at(%s) 回调异常: %s", hm, e)

                env.event_bus.add_listener(EVENT.BAR, _on_bar)
                if self._is_daily:
                    env.event_bus.add_listener(
                        EVENT.BEFORE_TRADING, lambda e: _fire_daily_at((9, 31)))
                    env.event_bus.add_listener(
                        EVENT.AFTER_TRADING, lambda e: _fire_daily_at(None, exclude=(9, 31)))

            def tear_down(self, *args):
                return

        return _JqBarCacheMod()

    mod.load_mod = load_mod
    mod.__config__ = {"base": {}, "mod": {}, "extra": {}}
    sys.modules["rqalpha_mod_jqbarcache"] = mod


# ---------------------------------------------------------------------------
# 补丁：Position / Portfolio / StrategyContext
# ---------------------------------------------------------------------------
def _patch_rqalpha_objects():
    from rqalpha.portfolio.position import Position, PositionProxy
    from rqalpha.portfolio import Portfolio
    from rqalpha.core.strategy_context import StrategyContext
    try:
        from rqalpha.mod.rqalpha_mod_sys_accounts.position_model import StockPositionProxy
    except Exception:
        StockPositionProxy = None

    def _tpatch(cls):
        if cls is None:
            return
        if not hasattr(cls, "total_amount"):
            cls.total_amount = property(lambda self: self.long.quantity)
        if not hasattr(cls, "closeable_amount"):
            cls.closeable_amount = property(lambda self: self.long.closable)
        if not hasattr(cls, "avg_cost"):
            cls.avg_cost = property(lambda self: self.long.avg_price)
        if not hasattr(cls, "price"):
            cls.price = property(lambda self: self.long.last_price)
        if not hasattr(cls, "value"):
            cls.value = property(lambda self: self.long.market_value)

    _tpatch(PositionProxy)
    _tpatch(StockPositionProxy)

    if not hasattr(Portfolio, "available_cash"):
        Portfolio.available_cash = property(lambda self: self.cash)

    if not hasattr(StrategyContext, "current_dt"):
        StrategyContext.current_dt = property(
            lambda self: Environment.get_instance().calendar_dt)

    if not hasattr(StrategyContext, "previous_date"):
        def _prev(self):
            env = Environment.get_instance()
            try:
                cal = env.data_proxy.get_trading_calendar()
                d = env.trading_dt.date()
                idx = int(cal.searchsorted(pd.Timestamp(d)))
                if idx > 0:
                    return cal[idx - 1]
            except Exception:
                pass
            return env.trading_dt.date()
        StrategyContext.previous_date = property(_prev)

    # 让 context.universe 可读写（聚宽允许策略设置自己的 universe，rqalpha 只读）
    _orig_universe = getattr(StrategyContext, "universe", None)
    def _get_universe(self):
        if hasattr(self, "_user_universe"):
            return self._user_universe
        if _orig_universe is not None:
            try:
                return _orig_universe.fget(self)
            except Exception:
                return []
        return []
    def _set_universe(self, val):
        self._user_universe = list(val)
    StrategyContext.universe = property(_get_universe, _set_universe)


# ---------------------------------------------------------------------------
# 安装入口
# ---------------------------------------------------------------------------
_JQDATA_MOD = None


def _register_jq_apis():
    """将聚宽兼容 shim 注册进 rqalpha.api 与假 jqdata 模块。

    必须在所有系统 mod 的 start_up 之后再次调用，因为 sys_accounts /
    sys_simulation 等会在各自 start_up 中用原生实现覆盖 api.__all__ 中的同名
    函数（如 get_price / set_option / run_daily）。我们的 quantbridge mod
    start_up 运行较晚，会兜底重新注册，确保策略 `from jqdata import *` 拿到的是
    我们的实现。
    """
    # 聚宽全局上下文对象 g：策略用 g.xxx = yyy 在全局持久共享状态。
    class _GlobalContext:
        pass
    _GLOBAL_G = _GlobalContext()

    global _JQDATA_MOD
    if _JQDATA_MOD is None:
        # 假 jqdata 模块，使 `from jqdata import *` 注入我们的兼容 shim。
        # 注意 backend/jqdata 是仅含数据文件的命名空间包，会抢先占据
        # sys.modules["jqdata"]，因此必须强制覆盖（不能用 setdefault）。
        _JQDATA_MOD = types.ModuleType("jqdata")
        sys.modules["jqdata"] = _JQDATA_MOD

    _shims = [
        ("g", _GLOBAL_G),
        ("get_price", get_price),
        ("get_all_securities", get_all_securities),
        ("get_current_data", get_current_data),
        ("get_security_name", get_security_name),
        ("get_security_info", get_security_info),
        ("attribute_history", attribute_history),
        ("is_temporarily_suspended", is_temporarily_suspended),
        ("get_trade_days", get_trade_days),
        ("set_benchmark", set_benchmark),
        ("set_slippage", set_slippage),
        ("set_order_cost", set_order_cost),
        ("set_option", set_option),
        ("history", history),
        ("record", record),
        ("run_daily", run_daily),
        ("run_minute", run_minute),
        ("log", _Log()),
        ("PriceRelatedSlippage", PriceRelatedSlippage),
        ("OrderCost", OrderCost),
    ]
    _all_names = []
    for name, fn in _shims:
        register_api(name, fn)
        setattr(_JQDATA_MOD, name, fn)
        _all_names.append(name)
    _JQDATA_MOD.__all__ = _all_names


def _patch_price_board():
    """让 rqalpha 的下单定价（get_last_price / 涨跌停）在标的未订阅时，
    回退到 DataManager 支撑的 1m/日线价格（与聚宽参考一致），避免全量订阅 1600+
    ETF 导致 OOM，同时使动态池 ETF 也能正常下单。幂等。
    """
    try:
        from rqalpha.data.bar_dict_price_board import BarDictPriceBoard
    except Exception:
        return

    if getattr(BarDictPriceBoard, "_jqcompat_patched", False):
        return

    _orig_glp = BarDictPriceBoard.get_last_price
    _orig_glu = BarDictPriceBoard.get_limit_up
    _orig_gld = BarDictPriceBoard.get_limit_down

    def _fallback_price(order_book_id, col):
        env = Environment.get_instance()
        for freq in ("1m", "1d"):
            try:
                bar = env.data_proxy.get_bar(order_book_id, env.trading_dt, freq)
                if bar is not None and bar[col] == bar[col]:
                    return float(bar[col])
            except Exception:
                continue
        return float("nan")

    def _glp(self, order_book_id):
        try:
            return _orig_glp(self, order_book_id)
        except Exception:
            return _fallback_price(order_book_id, "close")

    def _glu(self, order_book_id):
        # bar 的 limit_up 列已按昨收×(1+分档幅度) 计算（见 _limit_prices_from_prev_close
        # 与 _limit_rate），直接取列值；不再用 high×1.1 估算（会使涨跌停判断失真）。
        try:
            return _orig_glu(self, order_book_id)
        except Exception:
            return _fallback_price(order_book_id, "limit_up")

    def _gld(self, order_book_id):
        try:
            return _orig_gld(self, order_book_id)
        except Exception:
            return _fallback_price(order_book_id, "limit_down")

    BarDictPriceBoard.get_last_price = _glp
    BarDictPriceBoard.get_limit_up = _glu
    BarDictPriceBoard.get_limit_down = _gld
    BarDictPriceBoard._jqcompat_patched = True


def install_jqcompat(universe, names=None, benchmark="000300.XSHG", list_dates=None):
    """在 rqalpha.run() 之前调用：注册所有 shim 与补丁。

    每次调用全量重置模块级状态（宇宙/名称/基准/调度回调/当前 bar 快照），
    保证同进程连续多次回测互不串扰（否则第二次回测会继承上次的
    run_daily/every_bar 回调）。list_dates: {code: (list_date, delist_date)}
    （'YYYY-MM-DD' 字符串元组，可缺省），供 get_all_securities(date=...)
    过滤上市/退市标的。
    """
    global _UNIVERSE, _NAMES, _BENCHMARK, _LIST_DATES, _CURRENT_BAR_DICT
    _UNIVERSE = list(universe)
    _NAMES = dict(names or {})
    _BENCHMARK = benchmark
    _LIST_DATES = dict(list_dates or {})
    _EVERY_BAR_CALLBACKS.clear()
    _DAILY_AT.clear()
    _FQ_WARNED.clear()  # fq 复权提示按回测重置（每次回测每标的重新提示一次）
    _CURRENT_BAR_DICT = None

    _register_jq_apis()
    _patch_rqalpha_objects()
    _patch_price_board()
    _install_barcache_mod()
