"""聚宽兼容 API 子集。

在用户策略命名空间中注入本模块的函数，使原版聚宽策略（五福 ETF 轮动等）
可直接粘贴运行。运行期状态集中放在 ``_state``，由 :mod:`loader` 在每次
回测前重置。

撮合口径（与 ``app.quant.simulate.matcher`` 对齐）：
- 佣金双边收取（``_state["fee"]``，默认 CONFIG.fee_rate），无最低 5 元；
- 印花税仅卖出收取（A股 0.05%，ETF 免征，`QUANT_SIM_STAMP_TAX` 可覆盖）；
- 滑点双边（买 price*(1+slippage)，卖 price*(1-slippage)）；
- 买入 100 股整手（向下取整）；
- T+1：当日买入数量（Position.today_amount）当日不可卖，`on_new_day()` 清零；
- 涨跌停：``_state["no_buy"]`` / ``_state["no_sell"]`` 集合（由驱动方按轮注入）。
"""

import logging
import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday
from pandas.tseries.offsets import CustomBusinessDay

from ...datasource.base import DataSourceError
from . import jq_names
from .context import Context, G, Position
from .portfolio import Portfolio

_logger = logging.getLogger("jqengine.api")

# 印花税率（仅卖出，股票 0.05%，ETF 免征；与 simulate.matcher 同口径同环境变量）
DEFAULT_STAMP_TAX = float(os.environ.get("QUANT_SIM_STAMP_TAX", "0.0005"))


def _is_etf(code):
    """简单代码前缀判定：沪市 5 开头、深市 15/16 开头为基金（免印花税）。"""
    num = (code or "").split(".")[0]
    return num.startswith(("5", "15", "16"))

_state = {
    "ctx": None,
    "manager": None,
    "fee": 0.0003,
    "slippage": 0.001,
    "daily": [],
    "minute": [],
    "records": [],
    "trades": [],
}


def init(context):
    """用户可重写：策略初始化。默认空实现。"""
    pass


def run_daily(func, time="open"):
    """注册每日定时任务（time: 'open'/'close'/'every_bar' 或 'HH:MM'）。"""
    _state["daily"].append((func, time))


def run_minute(func, minute="every"):
    """注册每分钟定时任务。"""
    _state["minute"].append((func, minute))


def _filter_up_to(df, dt):
    """把 DataFrame 截到 <= current_dt，避免回测未来函数。

    日线（索引时间为 00:00）的当日 bar 代表 15:00 收盘——盘中（<15:00）
    取日线时不应包含当日（聚宽 attribute_history 恒不含当前 bar 语义）。
    比较基准 = 日线索引 + 15h（当日生效于收盘）；分钟线索引含具体时刻，原样比。
    """
    if df is None or df.empty:
        return df
    idx = None
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
    else:
        for c in ("datetime", "trade_date", "trade_time", "date", "time"):
            if c in df.columns:
                idx = pd.to_datetime(df[c], errors="coerce")
                break
    if idx is None:
        return df
    try:
        dt_ts = pd.Timestamp(dt)
        # 日线索引恒为 00:00：当日 bar 生效于 15:00，盘中不纳入当日
        cmp = idx
        if len(idx) and (idx.normalize() == idx).all():
            cmp = idx + pd.Timedelta(hours=15)
        return df[cmp <= dt_ts]
    except Exception:
        return df


def _default_snapshot(code):
    return SimpleNamespace(
        paused=False, last_price=0.0, day_open=0.0,
        high_limit=0.0, low_limit=0.0, amount=0, volume=0
    )


class CurrentDataProxy:
    """current_data 代理。

    日线级：last_price 取当日收盘（由 daily 数据决定），按 code 缓存。
    分钟级：静态字段（涨跌停价、是否停牌等）来自日线并缓存；last_price 每
    次访问都取桥接器维护的实时分钟价快照（``_state['minute_prices']``），
    快照中缺失的 code 再按需回看截至 current_dt 的最后一分钟收盘。
    """

    def __init__(self):
        self._daily = {}

    def _daily_info(self, code):
        if code in self._daily:
            return self._daily[code]
        info = self._default_daily(code)
        ctx = _state.get("ctx")
        dt = str(ctx.current_dt) if (ctx and ctx.current_dt is not None) else None
        mgr = _state.get("manager")
        if mgr and dt:
            try:
                start = (pd.Timestamp(dt) - pd.Timedelta(days=10)).strftime("%Y%m%d")
                df = mgr.fetch("get_daily", code, start, dt)
                if df is not None and not df.empty:
                    dt_ts = pd.Timestamp(dt)
                    if isinstance(df.index, pd.DatetimeIndex):
                        df = df[df.index <= dt_ts]
                    if df is not None and not df.empty:
                        row = df.iloc[-1]
                        prev_close = float(row.get("close", 0))
                        info = SimpleNamespace(
                            paused=False,
                            day_open=float(row.get("open", 0)),
                            high_limit=prev_close * 1.1,
                            low_limit=prev_close * 0.9,
                            amount=0,
                            volume=float(row.get("volume", 0)),
                        )
            except Exception:
                pass
        self._daily[code] = info
        return info

    def _default_daily(self, code):
        return SimpleNamespace(
            paused=False, day_open=0.0, high_limit=float("inf"),
            low_limit=0.0, amount=0, volume=0,
        )

    def _live_last_price(self, code):
        if not _state.get("minute_mode"):
            return None
        snap = _state.get("minute_prices") or {}
        if code in snap and snap[code]:
            return float(snap[code])
        mgr = _state.get("manager")
        ctx = _state.get("ctx")
        if mgr and ctx and ctx.current_dt is not None:
            # 直接对预加载分钟帧切片取末值，避免每次 get_price 重建整帧
            p = mgr.get_minute_price_at(code, ctx.current_dt)
            if p is not None:
                if code in ("159363.XSHE", "159381.XSHE"):
                    import sys
                    print(f"[DBG live] {code} dt={ctx.current_dt} price={p}", file=sys.stderr, flush=True)
                return p
        if ctx and ctx.current_dt is not None:
            try:
                df = get_price(code, count=1, frequency="1m")
                if df is not None and not df.empty:
                    p = float(df["close"].iloc[-1])
                    if code in ("159363.XSHE", "159381.XSHE"):
                        import sys
                        print(f"[DBG live] {code} dt={ctx.current_dt} price={p} shape={df.shape}", file=sys.stderr, flush=True)
                    return p
            except Exception as e:
                if code in ("159363.XSHE", "159381.XSHE"):
                    import sys
                    print(f"[DBG live] {code} dt={ctx.current_dt} EXC {e}", file=sys.stderr, flush=True)
                pass
        return None

    def __getitem__(self, code):
        info = self._daily_info(code)
        lp = self._live_last_price(code)
        last = lp if lp is not None else 0.0
        if code == "159363.XSHE" and _state.get("ctx") and _state["ctx"].current_dt:
            import sys
            dt = _state['ctx'].current_dt
            if hasattr(dt, 'strftime') and dt.strftime('%Y-%m-%d %H:%M') == '2026-04-20 13:10':
                print(f"[DBG cdata] {code} dt={dt} lp={lp} last={last} type={type(last)}", file=sys.stderr, flush=True)
        return SimpleNamespace(
            paused=info.paused, last_price=last, day_open=info.day_open,
            high_limit=info.high_limit, low_limit=info.low_limit,
            amount=info.amount, volume=info.volume,
        )

    def get(self, code, default=None):
        try:
            return self[code]
        except Exception:
            return default or _default_snapshot(code)


_current_data_proxy = None


def get_current_data():
    global _current_data_proxy
    if _current_data_proxy is None:
        _current_data_proxy = CurrentDataProxy()
    return _current_data_proxy


def clear_current_data_cache():
    """分钟级回测每个交易日清空静态日线缓存（last_price 始终实时，无需清）。"""
    global _current_data_proxy
    if _current_data_proxy is not None:
        _current_data_proxy._daily.clear()


def _get_price_batch_daily(security, start_date, end_date, count, fields, panel):
    """批量日线查询：直接从 _daily_mem 取，避免逐标的 copy/filter/reset_index。"""
    mgr = _state["manager"]
    ctx = _state.get("ctx")
    current_dt = ctx.current_dt if ctx and ctx.current_dt else None
    start_ts = pd.Timestamp(start_date).normalize() if start_date else None
    end_ts = pd.Timestamp(end_date).normalize() if end_date else None
    if current_dt:
        cutoff = pd.Timestamp(current_dt)
    else:
        cutoff = end_ts + pd.Timedelta(days=1) if end_ts else None

    frames = []
    for sec in security:
        df = mgr._daily_mem.get(f"get_daily_{sec}")
        if df is None or (hasattr(df, "empty") and df.empty):
            # 兜底：走原 fetch 路径（触发加载并缓存到 _daily_mem）。
            # 重试一次：回源瞬时失败曾导致单标的整体缺席动量计算
            # （08-21 黄金ETF误换仓根因之一），失败必须留痕告警。
            for attempt in (1, 2):
                try:
                    df = mgr.fetch("get_daily", sec, start_date or "20000101", end_date or "20300101")
                    break
                except Exception as e:
                    _logger.warning("批量日线取数失败(第%d次) %s: %s", attempt, sec, e)
                    df = None
            if df is None or (hasattr(df, "empty") and df.empty):
                _logger.warning("批量日线取数最终失败，标的缺席本次历史数据: %s", sec)
                continue
        idx = df.index
        if not isinstance(idx, pd.DatetimeIndex):
            continue
        mask = np.ones(len(idx), dtype=bool)
        norm = idx.normalize()
        if start_ts:
            mask &= (norm >= start_ts)
        if end_ts:
            mask &= (norm <= end_ts)
        if cutoff:
            mask &= (idx <= cutoff)
        sub = df[mask]
        if count and len(sub) > count:
            sub = sub.tail(count)
        if sub.empty:
            continue
        sub = sub.copy()
        sub["code"] = sec
        sub["time"] = sub.index
        frames.append(sub.reset_index(drop=True))
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    # 数值列转换
    for nc in ("open", "high", "low", "close", "volume", "money", "amount"):
        if nc in result.columns:
            result[nc] = pd.to_numeric(result[nc], errors="coerce")
    if fields:
        keep = [c for c in fields if c in result.columns]
        if "time" in result.columns:
            keep = ["time"] + keep
        if "code" in result.columns:
            keep = ["code"] + keep
        result = result[keep]
    if panel and len(security) == 1:
        single = result.set_index("time")
        single = single.drop(columns=["code"], errors="ignore")
        return single
    return result


def _get_price_batch_minute(security, start_date, end_date, count, fields, panel):
    """批量分钟线查询：直接从预加载的 _minute_mem 切片，避免逐标的 copy/reset_index。"""
    mgr = _state["manager"]
    ctx = _state.get("ctx")
    current_dt = ctx.current_dt if ctx and ctx.current_dt else None
    start_ts = pd.Timestamp(start_date) if start_date else None
    end_ts = pd.Timestamp(end_date) if end_date else None
    cutoff = pd.Timestamp(current_dt) if current_dt else (end_ts if end_ts else None)
    # 只复制需要的列（如 volume-only 查询就不复制 OHLC），大幅减少拷贝量
    want = list(fields) if fields else None

    frames = []
    for sec in security:
        df = mgr._minute_mem.get(sec)
        if df is None or (hasattr(df, "empty") and df.empty):
            # 兜底：走原 fetch 路径（触发加载并缓存到 _minute_mem）
            try:
                raw = mgr.get_minute(sec, end_date, start_date)
            except Exception:
                raw = None
            if raw is None or (hasattr(raw, "empty") and raw.empty):
                continue
            df = raw
        if not isinstance(df.index, pd.DatetimeIndex):
            continue
        mask = np.ones(len(df), dtype=bool)
        if start_ts is not None:
            mask &= (df.index >= start_ts)
        if end_ts is not None:
            mask &= (df.index <= end_ts)
        if cutoff is not None:
            mask &= (df.index <= cutoff)
        sub = df[mask]
        if count and len(sub) > count:
            sub = sub.iloc[-count:]
        if sub.empty:
            continue
        if want:
            cols = [c for c in want if c in sub.columns]
            s2 = sub[cols].copy()
        else:
            s2 = sub.copy()
        s2["code"] = sec
        s2["time"] = s2.index
        frames.append(s2.reset_index(drop=True))
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    for nc in ("open", "high", "low", "close", "volume", "money", "amount"):
        if nc in result.columns:
            result[nc] = pd.to_numeric(result[nc], errors="coerce")
    if fields:
        keep = [c for c in fields if c in result.columns]
        if "time" in result.columns:
            keep = ["time"] + keep
        if "code" in result.columns:
            keep = ["code"] + keep
        result = result[keep]
    if panel and len(security) == 1:
        single = result.set_index("time")
        single = single.drop(columns=["code"], errors="ignore")
        return single
    return result


def get_price(security, start_date=None, end_date=None, count=None,
               frequency="daily", fq="qfq", panel=True, fill_paused=True,
               fields=None, skip_paused=False):
    mgr = _state["manager"]
    if mgr is None:
        return pd.DataFrame()
    if isinstance(security, str):
        security = [security]
    # 快速批量路径：多标的 + 已预加载
    if len(security) > 10 and mgr._daily_mem and frequency not in ("1m", "minute", "1min"):
        return _get_price_batch_daily(security, start_date, end_date, count,
                                       fields, panel)
    if len(security) > 10 and mgr._minute_mem and frequency in ("1m", "minute", "1min"):
        return _get_price_batch_minute(security, start_date, end_date, count,
                                       fields, panel)
    dfs = []
    for sec in security:
        try:
            if frequency in ("1m", "minute", "1min"):
                _ctx = _state.get("ctx")
                _dt = end_date or start_date or (
                    _ctx.current_dt if _ctx and _ctx.current_dt else "")
                raw = mgr.fetch("get_minute", sec, _dt)
            else:
                raw = mgr.fetch("get_daily", sec,
                                start_date or "20000101", end_date or "20300101")
            if raw is None or raw.empty:
                continue
            # 高效切片：分钟单标的取数（count=1 等）用 searchsorted 直接定位末位，
            # 避免对整帧（数千行）做 normalize + 多次布尔掩膜遍历。
            sub = raw
            if isinstance(raw.index, pd.DatetimeIndex) and frequency in ("1m", "minute", "1min"):
                cand = []
                if end_date is not None:
                    cand.append(pd.Timestamp(end_date))
                _ctx = _state.get("ctx")
                if _ctx is not None and _ctx.current_dt is not None:
                    cand.append(pd.Timestamp(_ctx.current_dt))
                if cand:
                    bound = min(cand)
                    if bound == bound.normalize():
                        bound = bound + pd.Timedelta(hours=15)
                    pos = raw.index.searchsorted(bound, side="right") - 1
                    if pos < 0:
                        continue
                    if start_date is not None:
                        start_ts = pd.Timestamp(start_date).normalize()
                        lo = raw.index.searchsorted(start_ts, side="left")
                        if count:
                            lo = max(lo, pos - count + 1)
                        sub = raw.iloc[max(0, lo):pos + 1]
                    elif count:
                        sub = raw.iloc[max(0, pos - count + 1):pos + 1]
                    else:
                        sub = raw.iloc[:pos + 1]
                    df = sub.copy()
                else:
                    if start_date is not None:
                        sub = sub[sub.index.normalize() >= pd.Timestamp(start_date).normalize()]
                    if count and len(sub) > count:
                        sub = sub.iloc[-count:]
                    df = sub.copy()
            else:
                if isinstance(sub.index, pd.DatetimeIndex):
                    if start_date is not None:
                        sub = sub[sub.index.normalize() >= pd.Timestamp(start_date).normalize()]
                    if end_date is not None:
                        sub = sub[sub.index.normalize() <= pd.Timestamp(end_date).normalize()]
                    ctx = _state["ctx"]
                    if ctx is not None and ctx.current_dt is not None:
                        sub = _filter_up_to(sub, ctx.current_dt)
                if count and len(sub) > count:
                    sub = sub.iloc[-count:]
                df = sub.copy()
            if df.empty:
                continue
            for nc in ("open", "high", "low", "close", "volume",
                        "money", "amount", "vol"):
                if nc in df.columns:
                    df[nc] = pd.to_numeric(df[nc], errors="coerce")
            if isinstance(df.index, pd.DatetimeIndex) and df.index.name is None:
                df.index.name = "datetime"
            idx_name = df.index.name
            if idx_name and idx_name in df.columns:
                df = df.drop(columns=[idx_name])
            df = df.reset_index()
            time_col = None
            for c in ("datetime", "trade_date", "trade_time", "date", "time"):
                if c in df.columns:
                    time_col = c
                    break
            if time_col:
                df["time"] = pd.to_datetime(df[time_col], errors="coerce")
            else:
                df["time"] = pd.NaT
            df["code"] = sec
            if fields:
                keep = [c for c in fields if c in df.columns]
                if "time" in df.columns:
                    keep = ["time"] + keep
                if "code" in df.columns:
                    keep = ["code"] + keep
                df = df[keep]
            dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    result = pd.concat(dfs, ignore_index=True)
    if not panel and len(security) > 1:
        return result
    if panel and len(security) == 1:
        single = dfs[0]
        if "time" in single.columns:
            single = single.set_index("time")
            single = single.drop(columns=["code"], errors="ignore")
            for drop_c in ("datetime", "trade_date", "date"):
                if drop_c in single.columns:
                    single = single.drop(columns=[drop_c])
            return single
    return result


def get_extras(field, securities, start_date=None, end_date=None, count=None,
               frequency="daily", fields=None, skip_paused=True, fq="qfq",
               df=True, **kwargs):
    """聚宽 get_extras 兼容 shim（模拟盘引擎版）。

    与 ``app.quant.jqcompat.get_extras`` 同口径：当前仅支持
    field='unit_net_value'（ETF 真实净值），其余字段返回空并 warn。
    返回 DataFrame：行=日期，列=security，供策略 ``df.loc[date, code]`` 取用。
    """
    if field != "unit_net_value":
        _emit_sink("warn", f"[get_extras] field={field} 未实现，返回空 DataFrame")
        return pd.DataFrame()
    codes = [securities] if isinstance(securities, str) else list(securities)
    end_dt = pd.Timestamp(end_date) if end_date is not None else (
        getattr(_state.get("ctx"), "current_dt", None) or pd.Timestamp.now())
    navs = _get_etf_nav_df(codes, end_dt.date())
    if isinstance(securities, str):
        return navs[[codes[0]]] if codes[0] in navs.columns else pd.DataFrame()
    return navs


def _get_nav_manager():
    return _state.get("manager")


def _build_nav_frame(raw, codes: list[str], end_date) -> pd.DataFrame:
    """把 client 返回的 {code: df} 归一为 行=日期、列=security 的 DataFrame。"""
    if not raw:
        return pd.DataFrame()
    out = {}
    for code in codes:
        df = raw.get(code)
        if df is None or df.empty:
            continue
        idx = (pd.to_datetime(df["date"]) if "date" in df.columns
               else pd.to_datetime(df.index))
        out[code] = pd.Series(df["unit_nav"].values, index=idx)
    if not out:
        return pd.DataFrame()
    result = pd.DataFrame(out).sort_index()
    return result[result.index <= pd.Timestamp(end_date)]


def _get_etf_nav_df(codes: list[str], end_date) -> pd.DataFrame:
    """从 StockDataClient 拉真实单位净值：end_date 精确分区优先，缺失回退最新分区。

    先请求 ``get_etf_nav(codes, str(end_date))``；若全部标的为空（精确分区缺失），
    再请求 ``get_etf_nav(codes, None)``（最新分区）并保留 index <= end_date 的行。
    返回 DataFrame：行=日期，列=security。无数据返回空 DataFrame。
    """
    mgr = _get_nav_manager()
    if mgr is None or not hasattr(mgr, "client"):
        _emit_sink("warn", "[get_extras] 无可用 DataManager，返回空")
        return pd.DataFrame()
    result = _build_nav_frame(
        mgr.client.get_etf_nav(codes, str(end_date)), codes, end_date)
    if result.empty:
        result = _build_nav_frame(
            mgr.client.get_etf_nav(codes, None), codes, end_date)
    return result


def attribute_history(security, count, unit="1d", fields=None, skip_paused=True, df=True):
    freq_map = {"1d": "daily", "1m": "minute"}
    freq = freq_map.get(unit, "daily")
    fields = fields or ["close"]
    result = get_price(security, count=count, frequency=freq, fields=fields, panel=True)
    if isinstance(result, pd.DataFrame) and not result.empty:
        result = result.reset_index(drop=True)
        result.index = range(-len(result), 0)
        result.index.name = "date" if unit == "1d" else "datetime"
        return result
    return pd.DataFrame()


def _live_price(security):
    """取当前 bar 的实时价，回退到持仓价。

    回测桥每根 bar 都会把各标的当前价写入 minute_prices 快照（日线用
    close，分钟线用当前价），因此无论日线还是分钟模式都优先取该快照；
    分钟模式下再回退到 manager 精确取点；最后回退到持仓价。
    """
    snap = _state.get("minute_prices") or {}
    if security in snap and snap[security]:
        return snap[security]
    if _state.get("minute_mode"):
        mgr = _state.get("manager")
        ctx = _state.get("ctx")
        if mgr and ctx and ctx.current_dt is not None:
            p = mgr.get_minute_price_at(security, ctx.current_dt)
            if p is not None:
                if security in ("159363.XSHE", "159381.XSHE"):
                    import sys
                    print(f"[DBG price] {security} dt={ctx.current_dt} price={p}", file=sys.stderr, flush=True)
                return p
    return _state["ctx"].portfolio.get_position(security).price or 0


def order(security, amount):
    """按股数下单（正买负卖）。

    交易规则：买入 100 股整手；T+1（当日买入不可卖，卖出量按 closeable 截断）；
    佣金双边 + 卖出印花税（非 ETF）；``_state["no_buy"]/["no_sell"]`` 禁买卖。
    """
    ctx = _state["ctx"]
    p = ctx.portfolio
    price = _live_price(security)
    if price == 0 or amount == 0:
        return False
    amount = int(amount)
    if amount > 0:
        amount = amount // 100 * 100  # A股/ETF 买入整手（100 股向下取整）
        if amount <= 0 or security in (_state.get("no_buy") or ()):
            return False  # 不足一手 / 涨停禁买
    existing = p.positions.get(security)
    prev_cost = float(existing.avg_cost or 0.0) if existing else 0.0
    if amount < 0:
        if security in (_state.get("no_sell") or ()):
            return False  # 跌停/停牌禁卖
        closeable = float(existing.closeable_amount) if existing else 0.0
        amount = -min(-amount, closeable)  # T+1：卖出不超过可卖量
        if amount == 0:
            return False
    fee = _state["fee"]
    slip = _state["slippage"]
    fill = price * (1 + slip) if amount > 0 else price * (1 - slip)
    fill = round(fill, 3)
    turnover = abs(amount) * fill
    # Use separate buy/sell commission rates with minimum
    fee_cfg = _state.get("fee_config")
    if fee_cfg:
        if amount > 0:
            comm_rate = fee_cfg["open_commission"]
        else:
            comm_rate = fee_cfg["close_commission"]
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
    pos = p.positions.setdefault(security, Position())
    if amount > 0:
        if float(pos.amount or 0.0) <= 0:
            pos.entry_ts = ctx.current_dt  # 首次建仓记录买入时间
        total_cost = pos.amount * pos.avg_cost + amount * fill
        pos.amount += amount
        pos.avg_cost = total_cost / pos.amount if pos.amount else 0.0
        pos.today_amount = float(pos.today_amount or 0.0) + amount  # T+1 当日买入冻结
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
    """按金额下单。"""
    ctx = _state["ctx"]
    price = _live_price(security)
    if price == 0 or value == 0:
        return False
    amount = int(value // (price * (1 + _state["fee"])))
    return order(security, amount)


def order_target(security, amount):
    """调整持仓到目标股数（聚宽语义；amount=0 即清仓）。

    此前缺失，策略/桥接注入代码调用即 NameError 且常被静默吞掉
    （2026-08-22 账户级止损层注入首次暴露）。
    """
    ctx = _state["ctx"]
    p = ctx.portfolio
    pos = p.get_position(security)
    current = int(getattr(pos, "amount", 0) or 0)
    delta = int(amount) - current
    if delta == 0:
        return True
    return order(security, delta)


def order_target_percent(security, percent):
    """调整到目标仓位比例（0~1）。"""
    ctx = _state["ctx"]
    p = ctx.portfolio
    price = _live_price(security)
    if price == 0:
        return False
    target_value = p.value * percent
    target_shares = int(target_value // price) if percent > 0 else 0
    current_amount = p.get_position(security).amount
    amount = target_shares - current_amount
    if amount > 0:
        # 受手续费/滑点影响，买入不能超过可用资金
        unit_cost = price * (1 + _state["slippage"]) * (1 + _state["fee"])
        affordable = int(p.cash // unit_cost) if unit_cost > 0 else 0
        amount = min(amount, affordable)
    if amount != 0:
        return order(security, amount)
    return True


def _emit_sink(level, msg):
    """策略日志外送（``_state["log_sink"]``，由模拟盘 bridge 注入写库）。"""
    sink = _state.get("log_sink")
    if sink:
        try:
            sink(level, msg)
        except Exception:
            pass


class LogProxy:
    _levels = {"debug": 0, "info": 1, "warn": 2, "error": 3}
    _modules = {}

    def set_level(self, module, level):
        self._modules[module] = self._levels.get(level, 1)

    def _should(self, level_name):
        return self._modules.get("strategy", 1) <= self._levels[level_name]

    def info(self, msg):
        if self._should("info"):
            ctx = _state.get("ctx")
            dt = ctx.current_dt if ctx else ""
            print(f"[STRATEGY {dt}] {msg}")
            _emit_sink("info", msg)

    def warn(self, msg):
        if self._should("warn"):
            ctx = _state.get("ctx")
            dt = ctx.current_dt if ctx else ""
            print(f"[STRATEGY {dt}] [WARN] {msg}")
            _emit_sink("warn", msg)

    def warning(self, msg):
        self.warn(msg)

    def error(self, msg):
        if self._should("error"):
            ctx = _state.get("ctx")
            dt = ctx.current_dt if ctx else ""
            print(f"[STRATEGY {dt}] [ERROR] {msg}")
            _emit_sink("error", msg)

    def debug(self, msg):
        if self._should("debug"):
            ctx = _state.get("ctx")
            dt = ctx.current_dt if ctx else ""
            print(f"[STRATEGY {dt}] [DEBUG] {msg}")
            _emit_sink("debug", msg)

    def notify(self, msg):
        """推送通知（钉钉等）。未开启时退化为 log.info（写日志不推送）。"""
        self.info(msg)
        _emit_sink("notify", msg)

    def info_format(self, fmt, *args):
        self.info(fmt % args if args else fmt)

log = LogProxy()


def record(**kw):
    _state["records"].append(kw)


def _reset(manager, fee, slippage, cash):
    """回测前重置运行期状态，返回新建的 context。"""
    global _current_data_proxy
    _current_data_proxy = None
    ctx = Context()
    ctx.portfolio = Portfolio(cash)
    _state.update(ctx=ctx, manager=manager, fee=fee, slippage=slippage,
                  fee_config=None, daily=[], minute=[], records=[], trades=[],
                  minute_prices={}, minute_mode=False,
                  no_buy=set(), no_sell=set(), log_sink=None)
    return ctx


def on_new_day():
    """新交易日钩子：清零 T+1 当日买入冻结量，并清 current_data 静态日线缓存。"""
    ctx = _state.get("ctx")
    if ctx is not None and ctx.portfolio is not None:
        for pos in ctx.portfolio.positions.values():
            pos.today_amount = 0.0
    clear_current_data_cache()


def _state_snapshot():
    """供测试/调试查看当前状态。"""
    return dict(_state)


class PriceRelatedSlippage:
    def __init__(self, value):
        self.value = value
    def get(self, key, default=None):
        return getattr(self, key, default)


class OrderCost:
    def __init__(self, open_tax=0, close_tax=0, open_commission=0.0001,
                 close_commission=0.0001, close_today_commission=0.0001,
                 min_commission=5):
        self.open_tax = open_tax
        self.close_tax = close_tax
        self.open_commission = open_commission
        self.close_commission = close_commission
        self.close_today_commission = close_today_commission
        self.min_commission = min_commission
    def get(self, key, default=None):
        return getattr(self, key, default)


def set_option(key, value):
    _state.setdefault("options", {})[key] = value


def set_slippage(obj, _type="fund", type=None):
    _type = type or _type
    if isinstance(obj, PriceRelatedSlippage):
        _state["slippage"] = float(obj.value)
    elif isinstance(obj, dict):
        _state["slippage"] = float(obj.get("value", _state["slippage"]))


def set_order_cost(obj, _type="fund", type=None):
    _type = type or _type
    if isinstance(obj, OrderCost):
        _state["fee"] = float(obj.open_commission)
        _state["fee_config"] = {
            "open_commission": float(obj.open_commission),
            "close_commission": float(obj.close_commission),
            "close_today_commission": float(obj.close_today_commission),
            "min_commission": float(obj.min_commission),
            "open_tax": float(obj.open_tax),
            "close_tax": float(obj.close_tax),
        }
    elif isinstance(obj, dict):
        _state["fee"] = float(obj.get("open_commission", _state["fee"]))
        _state["fee_config"] = {
            "open_commission": float(obj.get("open_commission", 0.0001)),
            "close_commission": float(obj.get("close_commission", 0.0001)),
            "close_today_commission": float(obj.get("close_today_commission", 0.0001)),
            "min_commission": float(obj.get("min_commission", 5)),
            "open_tax": float(obj.get("open_tax", 0)),
            "close_tax": float(obj.get("close_tax", 0)),
        }


def set_benchmark(code):
    _state["benchmark"] = code


_A_SHARE_CALENDAR = None


def _get_calendar():
    global _A_SHARE_CALENDAR
    if _A_SHARE_CALENDAR is not None:
        return _A_SHARE_CALENDAR
    mgr = _state.get("manager")
    if mgr:
        try:
            df = mgr.fetch("get_daily", "000300.XSHG", "20000101", "20300101")
            if df is not None and not df.empty:
                _A_SHARE_CALENDAR = pd.DatetimeIndex(sorted(df.index.normalize()))
                return _A_SHARE_CALENDAR
        except Exception:
            pass
    class ChinaCalendar(AbstractHolidayCalendar):
        rules = [
            Holiday("NewYear", month=1, day=1, observance=nearest_workday),
            Holiday("SpringFestival1", month=1, day=1, observance=nearest_workday),
            Holiday("LaborDay", month=5, day=1, observance=nearest_workday),
            Holiday("NationalDay", month=10, day=1, observance=nearest_workday),
        ]
    _A_SHARE_CALENDAR = pd.bdate_range("2000-01-01", "2030-12-31", freq=CustomBusinessDay(calendar=ChinaCalendar()))
    return _A_SHARE_CALENDAR


def get_trade_days(start_date=None, end_date=None, count=None):
    cal = _get_calendar()
    if not isinstance(cal, pd.DatetimeIndex):
        cal = pd.DatetimeIndex(cal)
    if end_date and count:
        end = pd.Timestamp(str(end_date))
        mask = cal <= end
        return pd.DatetimeIndex(cal[mask][-count:])
    if start_date and end_date:
        start_ts = pd.Timestamp(str(start_date))
        end_ts = pd.Timestamp(str(end_date))
        mask = (cal >= start_ts) & (cal <= end_ts)
        return pd.DatetimeIndex(cal[mask])
    return pd.DatetimeIndex(cal)


def _name_source() -> str:
    """策略侧名称源：service=数据服务名称映射（默认，覆盖全量在市品种并随
    行情同步更新）；jq=旧静态 jq_names 表（仅显式设置时兼容使用）。

    2026-08-22 起默认改为 service：静态表长期不更新，缺失次新 ETF 导致其
    display_name 回退为代码串、行业分组键漂移，同一策略在不同环境选出完全
    不同的候选（7868a5ca vs ff8320ae 08-10 地产华宝分歧根因）。
    """
    try:
        from .... import db as _db
        src = (_db.get_quant_setting("sim_strategy_name_source") or "").strip().lower()
        return src or "service"
    except Exception:
        return "service"


def _jq_names() -> dict[str, str]:
    return jq_names.load_jq_names()


def get_security_name(code):
    if code == "159363.XSHE":
        ctx = _state.get("ctx")
        if ctx and ctx.current_dt and hasattr(ctx.current_dt, 'strftime'):
            dt_str = ctx.current_dt.strftime('%Y-%m-%d %H:%M')
            if dt_str == '2026-04-20 13:10':
                import sys
                cd = get_current_data()
                lp = cd[code].last_price if code in cd._daily or True else 0
                print(f"[DBG gsn] {code} dt={ctx.current_dt} cd_last_price={lp}", file=sys.stderr, flush=True)
    names = _state.get("sec_names")
    if names and code in names:
        return names[code]
    mgr = _state.get("manager")
    if mgr:
        # 先尝试网络源通达信简称（与 get_all_securities 同口径：网络异常时
        # jq 名替换块仍执行，保证 jq 名生效）
        mootdx_names = {}
        if "network" in mgr.sources:
            try:
                mootdx_names = mgr.sources["network"].get_stock_names()
            except Exception:
                pass
        if _name_source() == "jq":
            jq = _jq_names()
            if jq:
                mootdx_names = {c.split(".")[0]: n for c, n in jq.items()}
        pure = code.split(".")[0]
        if pure in mootdx_names:
            if names is None:
                names = {}
            names[code] = mootdx_names[pure]
            _state["sec_names"] = names
            return mootdx_names[pure]
        try:
            etfs = mgr.fetch("get_etf_list")
            if etfs:
                if names is None:
                    names = {}
                for item in etfs:
                    if isinstance(item, dict):
                        jq_code = _ts_code_to_jq_code(item.get("ts_code", ""))
                        names[jq_code] = item.get("name", jq_code)
                _state["sec_names"] = names
        except Exception:
            pass
    return names.get(code, code) if names else code


def _ts_code_to_jq_code(ts_code):
    """ts_code 格式 -> 聚宽代码 (159069.SZ -> 159069.XSHE)。"""
    if "." not in ts_code:
        return ts_code
    pure, suffix = ts_code.split(".")
    if suffix == "SZ":
        return f"{pure}.XSHE"
    if suffix == "SH":
        return f"{pure}.XSHG"
    return ts_code


def get_all_securities(types=None, date=None):
    mgr = _state.get("manager")
    if mgr is None:
        return pd.DataFrame()
    types = types or ["etf"]
    # 从网络源获取通达信简称（与聚宽 display_name 一致）
    mootdx_names = {}
    if "network" in mgr.sources:
        try:
            mootdx_names = mgr.sources["network"].get_stock_names()
        except Exception:
            pass
    if _name_source() == "jq":
        jq = _jq_names()
        if jq:
            mootdx_names = {c.split(".")[0]: n for c, n in jq.items()}
    records = []
    for t in types:
        try:
            if t == "etf":
                items = mgr.fetch("get_etf_list")
            elif t == "stock":
                items = mgr.fetch("get_stock_list")
            else:
                continue
            for item in items or []:
                if isinstance(item, str):
                    code = _ts_code_to_jq_code(item)
                    pure = code.split(".")[0]
                    name = mootdx_names.get(pure, code)
                    records.append({"code": code, "display_name": name,
                                    "name": name, "start_date": "2000-01-01",
                                    "end_date": "2200-01-01", "type": t})
                else:
                    ts_code = item.get("ts_code", "")
                    code = _ts_code_to_jq_code(ts_code)
                    pure = code.split(".")[0]
                    name = mootdx_names.get(pure, item.get("name", code))
                    list_date = item.get("list_date", "")
                    if list_date and len(str(list_date)) == 8:
                        sd = f"{list_date[:4]}-{list_date[4:6]}-{list_date[6:8]}"
                    else:
                        sd = "2000-01-01"
                    records.append({"code": code, "display_name": name,
                                    "name": name, "start_date": sd,
                                    "end_date": "2200-01-01", "type": t})
        except Exception:
            continue
    if not records:
        df = pd.DataFrame(columns=["display_name", "name", "start_date", "end_date", "type"])
        df.index.name = "code"
        return df
    df = pd.DataFrame(records).set_index("code")
    if date is not None:
        cutoff = str(date)[:10] if not isinstance(date, str) else date[:10]
        df = df[df["start_date"] <= cutoff]
    return df


def is_temporarily_suspended(code, context=None):
    """Check if a security is temporarily suspended at the current bar.

    Returns True if minute volume is 0 (no trades at current time),
    indicating the security is likely halted/suspended.
    """
    if not _state.get("minute_mode"):
        return False
    snap = _state.get("minute_prices") or {}
    if code in snap:
        return False
    mgr = _state.get("manager")
    ctx = _state.get("ctx")
    if mgr and ctx and ctx.current_dt is not None:
        p = mgr.get_minute_price_at(code, ctx.current_dt)
        if p is None:
            return True
    return False


def get_security_info(code):
    """返回标的简况。display_name 委托 get_security_name 的真实名称解析。

    旧实现把 ts_code 当键、代码前缀当名称（string 项存 ``item.split('.')[0]``），
    导致 ``get_security_info(c).display_name`` 常返回代码——策略 get_security_name
    兜底（如走弱期 etf_names_dict 为空）时买卖日志/通知就显示成 ``代码(代码)``。
    统一走 get_security_name（sec_names/jq名/网络名/etf_list 逐级解析）。
    """
    return SimpleNamespace(display_name=get_security_name(code))
