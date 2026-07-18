"""RQAlpha 桥接：自定义数据源 + 跑聚宽式策略 + 回收指标落库。

说明（rqalpha 6.2.1 适配）：
- `rqalpha.data.base_data_source.BaseDataSource` 在 6.2.1 中**没有**抽象方法
  （`__abstractmethods__` 为空），改用「注册 store + 重写少量方法」的方式实现，
  而非 brief 中假设的抽象方法集合。
- `rqalpha.run(config, source_code=...)` 接受的是 config 字典，且返回值是
  `{mod_name: mod_result}` 的字典（不是带 `.portfolio` 的单一结果对象），
  因此指标/净值/成交都从 `result["sys_analyser"]` 中回收。
- 自定义数据源通过内置 mod（`rqalpha_mod_quantbridge`）在 `start_up` 阶段调用
  `env.set_data_source(...)` 注入，避免 BaseDataSource 默认实现的 bundle 路径依赖。
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import sys
import types
import uuid

import numpy as np
import pandas as pd

from rqalpha.const import INSTRUMENT_TYPE, MARKET, SIDE, TRADING_CALENDAR_TYPE
from rqalpha.interface import ExchangeRate
from rqalpha.model.instrument import Instrument

from . import db
from .config import CONFIG, QuantConfig
# 涨跌停幅度分档公共函数（与 jqcompat 同口径复用，避免两处各写一份分档规则）
from .jqcompat import _is_st_name, _limit_rate

# rqalpha 6.2.1 兼容补丁：sys_analyser tear_down 仍使用 pandas 3 已移除的
# 'mode.use_inf_as_na' 选项（pd.option_context 直接抛 OptionError，导致有成交的
# 回测在收尾阶段整体判失败）。pandas 3 中 inf 已按 NA 语义处理，这里重新注册为
# 无副作用的占位选项（module 级 pandas 未导出 register_option，走内部 config API）。
try:  # noqa: BLE001
    from pandas._config import config as _pd_config

    _pd_config.register_option("mode.use_inf_as_na", False)
except Exception:  # noqa: BLE001
    pass

try:
    from rqalpha import subscribe_event
    from rqalpha.core.events import EVENT
except Exception:  # noqa: BLE001
    subscribe_event = None
    EVENT = None


_LIVE_RUN_ID = None

logger = logging.getLogger(__name__)


def _fund_instrument_type(code: str) -> str:
    """按代码段判定基金/证券类型（与 jqcompat._fund_instrument_type 同口径，随
    本文件既有的 _DayBarStore/_CalendarStore 等复制惯例保留本地副本）。

    上交所(XSHG)：51/56/58 → ETF，50 → LOF；深交所(XSHE)：15 → ETF，16 → LOF；
    其余 → CS。rqalpha 股票账户支持 ETF/LOF（INST_TYPE_IN_STOCK_ACCOUNT），
    sys_transaction_cost 只对 CS 收卖出印花税 —— ETF/LOF 现实免税，类型必须
    标对，否则回测多扣 0.05% 印花税。
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


def _norm_frequency(freq) -> str:
    """rqalpha 只认 '1d'/'1m'；容忍 UI 侧历史别名（'daily'/'minute' 等）。"""
    f = str(freq or "1d").lower()
    if f in ("daily", "day"):
        return "1d"
    if f in ("minute", "min"):
        return "1m"
    return f


class _AvgCostTracker:
    """移动平均成本法持仓成本跟踪：估算卖出成交的已实现盈亏。

    口径：买入摊薄均成本；卖出不改变剩余持仓均成本，pnl = (卖价 − 卖出前
    均成本) × 数量，pnl_pct = (卖价 − 均成本) / 均成本。无成本记录（如追踪
    开始前已持仓）时按卖价处理（pnl=0），不制造虚假盈亏。交易费用不计入
    pnl（DB 有独立 commission 列）。live 钩子与结果兜底回收共用同一口径。
    """

    def __init__(self):
        self._pos = {}  # code -> (qty, avg_cost)

    @staticmethod
    def _is_buy(side) -> bool:
        # str(SIDE.BUY) 形如 'SIDE.BUY'，兼容枚举与原字符串两种来源
        return side == SIDE.BUY or "BUY" in str(side)

    def on_trade(self, code, side, price, qty):
        held_qty, avg = self._pos.get(code, (0.0, 0.0))
        if self._is_buy(side):
            new_qty = held_qty + qty
            new_avg = ((held_qty * avg + qty * price) / new_qty) if new_qty > 0 else 0.0
            self._pos[code] = (new_qty, new_avg)
            return 0.0, 0.0
        if avg <= 0:
            avg = price
        pnl = (price - avg) * qty
        pnl_pct = (price - avg) / avg
        remain = max(held_qty - qty, 0.0)
        self._pos[code] = (remain, avg if remain > 0 else 0.0)
        return pnl, pnl_pct


def _now():
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 内部 store（duck-typed，仅需实现 BaseDataSource 注册时用到的方法）
# ---------------------------------------------------------------------------
# 日线 bar 的结构化数组 dtype（CS 证券 = 基础字段 + 涨跌停）
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
    def __init__(self, bars: dict):
        self._bars = bars

    def get_bars(self, order_book_id):
        return self._bars.get(order_book_id, np.empty(0, dtype=_BAR_DTYPE))

    def get_date_range(self, order_book_id):
        bars = self._bars.get(order_book_id)
        if bars is None or len(bars) == 0:
            return 20050104, 20050104
        return int(bars["datetime"][0]), int(bars["datetime"][-1])


class _CalendarStore:
    def __init__(self, dates):
        self._calendar = pd.DatetimeIndex(sorted(dates))

    def get_trading_calendar(self):
        return self._calendar


# ---------------------------------------------------------------------------
# 自定义数据源
# ---------------------------------------------------------------------------
class QuantRQAlphaDataSource:
    """基于 provider 的轻量 rqalpha 数据源。

    注意：此处**未**继承 BaseDataSource（6.2.1 中 BaseDataSource.__init__ 强依赖
    真实 bundle 路径与 .h5 文件）。改为 duck-typing 实现 DataProxy 实际调用的方法，
    以保证离线 mini bundle 可跑通。
    """

    def __init__(self, provider, config: QuantConfig, params: dict):
        self._provider = provider
        self._config = config
        self._params = params

        symbols = list(params.get("symbols") or [])
        start = params.get("start")
        end = params.get("end")

        self._bars: dict = {}
        self._instruments: dict = {}
        all_dates = set()

        # 交易日历来源：直接用缓存里所有日线数据的日期并集（不依赖
        # provider.get_daily，避免 offline 模式下缓存未命中即 raise 导致
        # 日历为空、回测报"区间内无数据"）。
        try:
            _all_daily = provider.cache.get_all("daily")
            for _k, _df in _all_daily.items():
                if _df is None or getattr(_df, "empty", True):
                    continue
                # 各源日线 schema 先归一（tushare trade_date/vol、mootdx datetime 索引），
                # 否则非 date 列命名的缓存帧被跳过、交易日历覆盖不足
                _df = self._normalize_daily_df(_df)
                if "date" not in _df.columns:
                    continue
                for _d in _df["date"]:
                    all_dates.add(pd.Timestamp(_d).date())
        except Exception:
            pass

        for code in symbols:
            try:
                df = provider.get_daily(code, start, end)
            except Exception:
                df = None
            if df is None or len(df) == 0:
                continue
            # 列名归一：tushare pro.daily 返回 trade_date/vol，mootdx 返回 datetime
            # 索引 + vol，与下方 date/volume 契约不一致会 KeyError 致整个 run failed
            df = self._normalize_daily_df(df)
            self._bars[code] = self._df_to_recarray(df, code=code)
            self._instruments[code] = self._make_instrument(code)
            for d in df["date"]:
                all_dates.add(pd.Timestamp(d).date())

        self._trading_dates = sorted(all_dates)

        # 注册 store（DataProxy 通过这些 store 取数）；
        # CS 与基金类型（ETF/LOF，见 _fund_instrument_type）共用同一套日线 store
        self._day_bar_stores = {}
        self._calendar_stores = {}
        for _t in (INSTRUMENT_TYPE.CS, INSTRUMENT_TYPE.ETF, INSTRUMENT_TYPE.LOF):
            self.register_day_bar_store(_t, _DayBarStore(self._bars), market=MARKET.CN)
        self.register_calendar_store(
            TRADING_CALENDAR_TYPE.CN_STOCK, _CalendarStore(self._trading_dates)
        )

    # ---- store 注册（与 BaseDataSource 同名方法，供内部复用） ----
    def register_day_bar_store(self, instrument_type, store, market=MARKET.CN):
        self._day_bar_stores[(instrument_type, market)] = store

    def register_calendar_store(self, calendar_type, store):
        self._calendar_stores[calendar_type] = store

    # ---- 内部构造辅助 ----
    @staticmethod
    def _normalize_daily_df(df: pd.DataFrame) -> pd.DataFrame:
        """各数据源日线 schema 归一到桥接契约（date/volume 列）。

        - tushare pro.daily：trade_date → date；vol → volume（tushare vol 单位为手，
          ×100 转股，与 mootdx 源内 vol×100 的口径一致）；
        - mootdx：datetime 索引 → date 列（其源已自行补 volume，不再重复换算）；
        - 已有 date/volume 列（如 bundle CSV）：原样返回，不改变既有行为。
        """
        if "date" not in df.columns:
            if "trade_date" in df.columns:
                df = df.rename(columns={"trade_date": "date"})
            elif isinstance(df.index, pd.DatetimeIndex):
                df = df.assign(date=df.index)
            elif "datetime" in df.columns:
                df = df.rename(columns={"datetime": "date"})
        if "volume" not in df.columns and "vol" in df.columns:
            df = df.assign(volume=pd.to_numeric(df["vol"], errors="coerce") * 100)
        return df

    @staticmethod
    def _df_to_recarray(df: pd.DataFrame, code: str = None):
        df = QuantRQAlphaDataSource._normalize_daily_df(df)
        df = df.sort_values("date").reset_index(drop=True)
        n = len(df)
        arr = np.zeros(n, dtype=_BAR_DTYPE)
        arr["datetime"] = df["date"].map(
            lambda d: int(pd.Timestamp(d).strftime("%Y%m%d"))
        ).to_numpy()
        arr["open"] = df["open"].to_numpy(dtype=np.float64)
        arr["close"] = df["close"].to_numpy(dtype=np.float64)
        arr["high"] = df["high"].to_numpy(dtype=np.float64)
        arr["low"] = df["low"].to_numpy(dtype=np.float64)
        arr["volume"] = df["volume"].to_numpy(dtype=np.float64)
        arr["total_turnover"] = (
            df["close"].to_numpy(dtype=np.float64) * df["volume"].to_numpy(dtype=np.float64)
        )
        # 涨跌停价按昨收计算（交易所口径）：round(prev_close×(1±rate), 2)，
        # rate 按代码分档（科创/创业 20cm、ST 5%、主板 10%，见 jqcompat._limit_rate）。
        # 首日无前收 → NaN（不用当日 open 估算，避免虚假涨跌停触发；NaN 比较
        # 恒 False，行为可预期）。code 缺省时按主板 10% 兜底。
        prev_close = df["close"].shift(1)
        rate = _limit_rate(code) if code else 0.10
        arr["limit_up"] = (prev_close * (1 + rate)).round(2).to_numpy(dtype=np.float64)
        arr["limit_down"] = (prev_close * (1 - rate)).round(2).to_numpy(dtype=np.float64)
        return arr

    def _make_instrument(self, code: str) -> Instrument:
        return Instrument(
            {
                "order_book_id": code,
                "symbol": code.split(".")[0],
                "type": _fund_instrument_type(code),
                "round_lot": 100,
                "board_type": "MAIN",
                "exchange": code.split(".")[1] if "." in code else "XSHG",
                "listed_date": "2000-01-01",
                "de_listed_date": "2999-12-31",
                "status": "Active",
                "special_type": None,
                "market_tplus": 1,
            },
            market=MARKET.CN,
        )

    # ---- DataProxy 实际调用的方法 ----
    def get_instruments(self, id_or_syms=None, types=None):
        if id_or_syms is not None:
            for i in id_or_syms:
                ins = self._instruments.get(i)
                if ins is not None:
                    yield ins
        else:
            yield from self._instruments.values()

    def get_trading_calendars(self):
        return {
            t: store.get_trading_calendar()
            for t, store in self._calendar_stores.items()
        }

    def available_data_range(self, frequency):
        if not self._trading_dates:
            return _dt.date.min, _dt.date.max
        logger.debug("available_data_range end=%s n=%d",
                     self._trading_dates[-1], len(self._trading_dates))
        return self._trading_dates[0], self._trading_dates[-1]

    def get_yield_curve(self, start_date, end_date, tenor=None):
        return None

    def is_suspended(self, order_book_id, dates):
        """停牌判定：该交易日日线 bar 缺失或 volume==0 判 True（self._bars 逐日查）。"""
        bars = self._bars.get(order_book_id)
        vol_by_day = {}
        if bars is not None and len(bars):
            for _dt_int, _vol in zip(bars["datetime"], bars["volume"]):
                vol_by_day[int(_dt_int)] = float(_vol)
        out = []
        for d in dates:
            day = int(pd.Timestamp(d).strftime("%Y%m%d"))
            vol = vol_by_day.get(day)
            out.append(vol is None or vol == 0.0)
        return out

    def is_st_stock(self, order_book_id, dates):
        """ST 判定：按名称含 "ST"（jqcompat._is_st_name，名称取 jqcompat._NAMES
        映射）；取不到名称时按非 ST。"""
        return [_is_st_name(order_book_id)] * len(dates)

    def get_ex_cum_factor(self, instrument):
        return None

    def get_dividend(self, instrument):
        return None

    def get_split(self, instrument):
        return None

    def get_merge_ticks(self, order_book_id_list, trading_date, last_dt=None):
        raise NotImplementedError

    def history_ticks(self, instrument, count, dt):
        raise NotImplementedError

    def get_algo_bar(self, id_or_ins, start_min, end_min, dt):
        raise NotImplementedError

    def get_open_auction_volume(self, instrument, dt):
        return 0

    def get_trading_minutes_for(self, instrument, trading_dt):
        raise NotImplementedError

    def append_suspend_date_set(self, date_set):
        pass

    def get_share_transformation(self, order_book_id):
        return None

    def get_exchange_rate(self, trading_date, local, settlement=MARKET.CN):
        # 单一市场（CN）下汇率恒为 1
        return ExchangeRate(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

    def get_settle_price(self, instrument, date):
        bar = self.get_bar(instrument, date, "1d")
        if bar is None:
            return float("nan")
        return float(bar["close"])

    def get_open_auction_bar(self, instrument, dt):
        bar = self.get_bar(instrument, dt, "1d")
        if bar is None:
            return None
        return bar

    def current_snapshot(self, instrument, frequency, dt):
        from rqalpha.data.data_proxy import TickObject

        bar = self.get_bar(instrument, dt, "1d")
        if bar is None:
            return None
        d = {
            "datetime": int(bar["datetime"]),
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "last": float(bar["close"]),
            "volume": float(bar["volume"]),
            "total_turnover": float(bar["total_turnover"]),
            "prev_close": float(bar["close"]),
            "limit_up": float(bar["limit_up"]),
            "limit_down": float(bar["limit_down"]),
        }
        return TickObject(instrument, d)

    # history_bars / get_bar / current_snapshot 依赖 store，由下方复用 BaseDataSource 实现
    def _all_day_bars_of(self, instrument):
        return self._day_bar_stores[(instrument.type, instrument.market)].get_bars(
            instrument.order_book_id
        )

    def history_bars(self, instrument, bar_count, frequency, fields, dt, **kwargs):
        if frequency not in ("1d", "1w"):
            raise NotImplementedError
        bars = self._all_day_bars_of(instrument)
        if len(bars) <= 0:
            return bars
        # 盘中防前视：dt 处于盘中时刻（(00:00, 15:00) 开区间）且 include_now=False 时
        # 不含 dt 当日 bar（对齐聚宽 attribute_history/history_bars 默认语义）。
        # dt 为纯日期（00:00）时按 rqalpha 原生口径含当日——sys_analyser 等内部调用
        # 以 dt=末日日期取基准日线，排除当日会让基准序列缺末日 bar 直接报错；
        # 15:00 起当日日线已完整，日线频率回测 handle_bar 于 15:00 触发，含当日。
        # 策略侧聚宽语义（00:00/盘中不含当日）由 jqcompat 数据源 shim 保证，不经此处。
        include_now = bool(kwargs.get("include_now", False))
        _t = pd.Timestamp(dt)
        _hms = (_t.hour, _t.minute, _t.second)
        _intraday = (0, 0, 0) < _hms < (15, 0, 0)
        side = "left" if (_intraday and not include_now) else "right"
        i = bars["datetime"].searchsorted(np.uint64(int(pd.Timestamp(dt).strftime("%Y%m%d"))), side=side)
        if bar_count is None:
            left = 0
        else:
            left = i - bar_count if i >= bar_count else 0
        bars = bars[left:i]
        if fields is None:
            return bars
        if isinstance(fields, str):
            return bars[fields]
        return bars[[f for f in fields if f in bars.dtype.names]]

    def get_bar(self, instrument, dt, frequency="1d"):
        if frequency != "1d":
            raise NotImplementedError
        bars = self._all_day_bars_of(instrument)
        if len(bars) <= 0:
            return None
        dt_int = np.uint64(int(pd.Timestamp(dt).strftime("%Y%m%d")))
        pos = bars["datetime"].searchsorted(dt_int)
        if pos >= len(bars) or bars["datetime"][pos] != dt_int:
            return None
        return bars[pos]


# ---------------------------------------------------------------------------
# 内置 mod：在 start_up 时注入自定义数据源
# ---------------------------------------------------------------------------
_PENDING_DATA_SOURCE = None


def _set_pending_data_source(ds):
    global _PENDING_DATA_SOURCE
    _PENDING_DATA_SOURCE = ds


def _consume_pending_data_source():
    global _PENDING_DATA_SOURCE
    ds = _PENDING_DATA_SOURCE
    _PENDING_DATA_SOURCE = None
    return ds


def _install_bridge_mod():
    if "rqalpha_mod_quantbridge" in sys.modules:
        return
    mod = types.ModuleType("rqalpha_mod_quantbridge")

    def load_mod():
        from rqalpha.interface import AbstractMod
        from app.quant import jqcompat as _jq

        class _QuantBridgeMod(AbstractMod):
            def start_up(self, env, mod_config):
                # 系统 mod（sys_accounts/sys_simulation 等）已在各自 start_up
                # 用原生实现覆盖了 api 同名函数，这里兜底重新注册我们的 shim，
                # 确保策略 `from jqdata import *` 拿到的是兼容层实现。
                _jq._register_jq_apis()
                # UI 路径同样需要 rqalpha 对象补丁：否则策略里
                # context.universe = [...] 抛 AttributeError（StrategyContext.universe
                # 原生只读）；PriceBoard 补丁保证未订阅标的的定价/涨跌停可回退取数。
                _jq._patch_rqalpha_objects()
                _jq._patch_price_board()
                ds = _consume_pending_data_source()
                if ds is not None:
                    env.set_data_source(ds)

            def tear_down(self, *args):
                return

        return _QuantBridgeMod()

    mod.load_mod = load_mod
    mod.__config__ = {
        "base": {},
        "mod": {"sys_risk": {}},
        "extra": {},
        "priority": 900,
    }
    sys.modules["rqalpha_mod_quantbridge"] = mod


_install_bridge_mod()


# ---------------------------------------------------------------------------
# 实时写库 mod（LiveStreamMod）：运行期把日志/收益/交易即时落 quant.db，
# 供 SSE 增量推送。结果 DB 始终是最新真值，SSE 断线后可凭轮询接口恢复。
# ---------------------------------------------------------------------------
def _install_live_mod():
    if "rqalpha_mod_quantlive" in sys.modules:
        return

    mod = types.ModuleType("rqalpha_mod_quantlive")

    def load_mod():
        from rqalpha.interface import AbstractMod

        class _LiveHandler(logging.Handler):
            def emit(self, record):
                if _LIVE_RUN_ID is None:
                    return
                try:
                    db.insert_log(_LIVE_RUN_ID, _now(), record.levelname, self.format(record))
                except Exception:  # noqa: BLE001
                    pass

        class _LiveMod(AbstractMod):
            def start_up(self, env, mod_config):
                self._env = env
                self._handlers = []
                # 基准净值基准日收盘价（首日收盘为 1.0）；卖出 pnl 成本跟踪
                self._bench_base_close = None
                self._cost_tracker = _AvgCostTracker()
                if EVENT is not None:
                    env.event_bus.add_listener(EVENT.AFTER_TRADING, self._on_after_trading)
                    env.event_bus.add_listener(EVENT.TRADE, self._on_trade)
                # 捕获策略日志：经 jqcompat.LIVE_SINK 实时写库（最稳，绕开
                # logbook/stdlib 路由差异）；同时挂一个 stdlib handler 作备份。
                try:
                    import logging as _logging

                    import app.quant.jqcompat as _jq

                    _jq.LIVE_SINK = lambda level, msg: (
                        db.insert_log(_LIVE_RUN_ID, _now(), level, msg)
                        if _LIVE_RUN_ID is not None else None
                    )
                    self._handlers.append(("sink", _jq, None))

                    h = _LiveHandler()
                    h.setLevel(_logging.INFO)
                    h.setFormatter(_logging.Formatter("%(message)s"))
                    _jq.logger.setLevel(_logging.INFO)
                    _jq.logger.addHandler(h)
                    self._handlers.append(("logging", _jq.logger, h))
                except Exception:  # noqa: BLE001
                    pass

            def _benchmark_nav(self, dt):
                """基准净值：取基准当日日线收盘价 / 首个收盘日收盘价。

                未配置基准（UI 路径 benchmark=None）或取数失败时统一占位 1.0，
                与兜底回收（_extract_equity）语义一致。
                """
                bench_code = getattr(getattr(self._env, "config", None), "base", None)
                bench_code = getattr(bench_code, "benchmark", None)
                if not bench_code:
                    return 1.0
                try:
                    ins = self._env.data_proxy.get_instrument(bench_code)
                    bar = self._env.data_proxy.get_bar(ins, dt, "1d")
                    if bar is None:
                        return 1.0
                    close = float(bar["close"])
                    if self._bench_base_close is None:
                        self._bench_base_close = close
                    if not self._bench_base_close:
                        return 1.0
                    return close / self._bench_base_close
                except Exception:  # noqa: BLE001
                    return 1.0

            def _on_after_trading(self, event):
                if _LIVE_RUN_ID is None:
                    return
                try:
                    acct = self._env.portfolio.stock_account
                    dt = event.trading_dt
                    dt_s = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)
                    db.insert_equity_row(
                        _LIVE_RUN_ID, dt_s,
                        float(getattr(acct, "total_value", 0.0) or 0.0),
                        self._benchmark_nav(dt),
                        float(getattr(acct, "cash", 0.0) or 0.0),
                        float(getattr(acct, "market_value", 0.0) or 0.0),
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("live equity 写入失败")

            def _on_trade(self, event):
                if _LIVE_RUN_ID is None:
                    return
                try:
                    t = event.trade
                    dt = t.datetime
                    ts = dt.strftime("%Y-%m-%d %H:%M") if hasattr(dt, "strftime") else str(dt)
                    price = float(getattr(t, "last_price", 0.0) or 0.0)
                    qty = float(getattr(t, "last_quantity", 0.0) or 0.0)
                    # 移动平均成本法估算已实现盈亏（买入为 0；口径见 _AvgCostTracker）
                    pnl, pnl_pct = self._cost_tracker.on_trade(
                        str(t.order_book_id), getattr(t, "side", None), price, qty)
                    db.insert_trade(
                        _LIVE_RUN_ID, ts,
                        str(t.order_book_id),
                        str(t.side),
                        price,
                        qty,
                        pnl, pnl_pct,
                        float(getattr(t, "transaction_cost", 0.0) or 0.0),
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("live trade 写入失败")

            def tear_down(self, *args):
                for kind, target, h in getattr(self, "_handlers", []):
                    try:
                        if kind == "sink":
                            target.LIVE_SINK = None
                        elif kind == "logging":
                            target.removeHandler(h)
                    except Exception:  # noqa: BLE001
                        pass
                self._handlers = []

        return _LiveMod()

    mod.load_mod = load_mod
    mod.__config__ = {
        "base": {},
        "mod": {"sys_risk": {}},
        "extra": {},
        "priority": 50,
    }
    sys.modules["rqalpha_mod_quantlive"] = mod


_install_live_mod()



# ---------------------------------------------------------------------------
# provider：从 bundle 目录读 CSV（无网络）
# ---------------------------------------------------------------------------
class _BundleProvider:
    def __init__(self, bundle_dir: str):
        self._dir = bundle_dir

    def get_daily(self, code, start, end):
        # 文件名使用完整 order_book_id（如 600000.XSHG.csv），与 brief 中
        # `code.split('.')[0]` 不同（brief 的命名假设与 fixture 不一致）。
        path = os.path.join(self._dir, "bars", "{}.csv".format(code))
        if not os.path.exists(path):
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        df = pd.read_csv(path)
        if start is not None:
            df = df[pd.to_datetime(df["date"]) >= pd.Timestamp(start)]
        if end is not None:
            df = df[pd.to_datetime(df["date"]) <= pd.Timestamp(end)]
        return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 结果回收
# ---------------------------------------------------------------------------
def _extract_metrics(result):
    try:
        summary = result["sys_analyser"]["summary"]

        def _num(v):
            v = float(v if v is not None else 0.0)
            # NaN/Inf 不是合法 JSON，转 None 避免前端 JSON.parse 失败
            if v != v or v in (float("inf"), float("-inf")):
                return None
            return v

        return {
            "total_return": _num(summary.get("total_returns")),
            "annualized": _num(summary.get("annualized_returns")),
            "sharpe": _num(summary.get("sharpe")),
            "max_drawdown": _num(summary.get("max_drawdown")),
        }
    except Exception:
        return {}


def _extract_equity(result):
    """回收每日净值。第 3 列语义为**基准净值**（与 live 钩子 insert_equity_row 一致）：
    sys_analyser 含 benchmark_portfolio 时取其 unit_net_value（按日期对齐、前向填充）；
    未配置基准或取数失败时统一占位 1.0。（此前兜底路径写的是组合 unit_net_value，
    与 live 钩子的 1.0 占位语义不一致，且占用 benchmark 列名，已统一。）
    """
    try:
        pf = result["sys_analyser"]["portfolio"]
        bench_nav = None
        bpf = result["sys_analyser"].get("benchmark_portfolio")
        if bpf is not None and "unit_net_value" in getattr(bpf, "columns", []):
            bench_nav = bpf["unit_net_value"].reindex(pf.index).ffill()
        out = []
        for i, (d, row) in enumerate(pf.iterrows()):
            dt = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
            bv = 1.0
            if bench_nav is not None:
                _v = float(bench_nav.iloc[i])
                bv = _v if _v == _v else 1.0  # 前段 NaN（基准数据未覆盖）占位 1.0
            out.append((
                dt,
                float(row.get("total_value", 0.0) or 0.0),
                bv,
                float(row.get("cash", 0.0) or 0.0),
                float(row.get("market_value", 0.0) or 0.0),
            ))
        return out
    except Exception:
        return []


def _extract_trades(result):
    """回收成交明细。pnl/pnl_pct 按移动平均成本法重放成交序列估算（与 live
    钩子同口径，见 _AvgCostTracker）；买入行为 0。"""
    out = []
    try:
        trades = result["sys_analyser"]["trades"]
        tracker = _AvgCostTracker()
        for _, t in trades.iterrows():
            dt = t["datetime"]
            ts = dt.strftime("%Y-%m-%d %H:%M") if hasattr(dt, "strftime") else str(dt)
            price = float(t.get("last_price", 0.0) or 0.0)
            qty = float(t.get("last_quantity", 0.0) or 0.0)
            pnl, pnl_pct = tracker.on_trade(str(t["order_book_id"]), t["side"], price, qty)
            out.append((
                ts,
                str(t["order_book_id"]),
                str(t["side"]),
                price,
                qty,
                pnl,
                pnl_pct,
                float(t.get("transaction_cost", 0.0) or 0.0),
            ))
    except Exception:
        pass
    return out


# ---------------------------------------------------------------------------
# 运行入口
# ---------------------------------------------------------------------------
def _build_config(params: dict) -> dict:
    return {
        "base": {
            "start_date": params["start"],
            "end_date": params["end"],
            # 回测频率由调用方传入（默认日线；rqalpha 只认 1d/1m，别名做归一）
            "frequency": _norm_frequency(params.get("frequency", "1d")),
            "run_type": "b",
            "accounts": {"stock": float(params.get("capital", 100000.0))},
            "benchmark": None,
            "data_bundle_path": params.get("bundle_dir") or _dt_dir(),
            "matching_type": "current_bar",
            "strategy_file": "strategy.py",
        },
        "mod": {
            "sys_analyser": {"record": True, "benchmark": None},
            "sys_simulation": {
                "slippage": float(params.get("slippage", 0.0)),
                "matching_type": "current_bar",
                "price_limit": False,
                "volume_limit": False,
                "inactive_limit": False,
            },
            "sys_transaction_cost": {
                # rqalpha 6.2.1 只认 stock_* 前缀键（旧键 commission_multiplier /
                # min_commission 为死键，会被忽略并退回默认万8）
                "stock_commission_multiplier": float(params.get("fee", 0.0003)) / 0.0008,
                "stock_min_commission": float(params.get("min_commission", 5)),
            },
            "quantbridge": {"enabled": True},
            # run_daily/every_bar 调度器（jqcompat bar 缓存 mod），无回调时近乎零开销
            "jqbarcache": {"enabled": True},
        },
        "extra": {"log_level": "error"},
    }


def _dt_dir() -> str:
    import tempfile

    d = os.path.join(tempfile.gettempdir(), "rqalpha_bridge_bundle")
    os.makedirs(d, exist_ok=True)
    return d


def run_backtest(strategy_code: str, params: dict, provider=None, db_path: str | None = None) -> dict:
    """跑回测并写 quant.db。返回 {run_id, metrics}。

    运行期由 LiveStreamMod 实时写日志/收益/交易；结束时仅兜底补写
    （若实时钩子未触发），并落 metrics + status=done/failed。
    """
    global _LIVE_RUN_ID
    if db_path:
        db.init_db(db_path)
    run_id = params.get("run_id") or uuid.uuid4().hex[:8]
    db.upsert_run(run_id, params.get("strategy_id", ""), params.get("name", ""), json.dumps(params, ensure_ascii=False), "running")
    _LIVE_RUN_ID = run_id
    try:
        from rqalpha import run as rq_run

        # UI 路径同样启用 jqbarcache 调度（run_daily/every_bar），需保证
        # rqalpha_mod_jqbarcache 已注入 sys.modules，否则 mod 加载失败
        from .jqcompat import _install_barcache_mod
        _install_barcache_mod()

        if provider is None:
            provider = _BundleProvider(params["bundle_dir"])
        ds = QuantRQAlphaDataSource(provider, CONFIG, params)
        _set_pending_data_source(ds)

        config = _build_config(params)
        config.setdefault("mod", {})["quantlive"] = {"enabled": True}
        result = rq_run(config, source_code=strategy_code)

        metrics = _extract_metrics(result)
        equity = _extract_equity(result)
        trades = _extract_trades(result)

        # 兜底：仅当实时钩子未写入时才全量补写（避免重复）
        if not db.get_equity(run_id):
            db.bulk_insert_equity(run_id, equity)
        if not db.get_trades(run_id):
            for t in trades:
                db.insert_trade(run_id, *t)
        # 用户 terminate 等外部操作可能已把 run 置为终态（failed/cancelled）：
        # 不覆盖终态，仅记录日志（metrics 丢弃，避免状态回跳）
        _cur = db.get_run(run_id) or {}
        if _cur.get("status") in ("failed", "cancelled"):
            logger.info("run %s 已是终态 %s，跳过 done 回写", run_id, _cur.get("status"))
        else:
            db.update_run(
                run_id, "done",
                metrics_json=json.dumps(metrics, ensure_ascii=False),
                finished_at=_now(),
            )
        return {"run_id": run_id, "metrics": metrics}
    except Exception as e:  # noqa: BLE001
        logger.exception("回测失败 run=%s", run_id)
        db.insert_log(run_id, _now(), "ERROR", str(e))
        db.update_run(run_id, "failed", error=str(e)[:500], finished_at=_now())
        return {"run_id": run_id, "error": str(e)}
    finally:
        _LIVE_RUN_ID = None


def run_backtest_on_bundle(bundle_dir, strategy_code, params, db_path=None) -> dict:
    """测试辅助：用 fixture bundle 直接构造 provider 跑（无网络）。"""
    params = dict(params)
    params["bundle_dir"] = bundle_dir
    if "symbols" not in params:
        params["symbols"] = ["600000.XSHG"]
    return run_backtest(strategy_code, params, provider=_BundleProvider(bundle_dir), db_path=db_path)


# ---------------------------------------------------------------------------
# 聚宽(wufu) 策略运行入口：注入 jqcompat + JqDataSource，跑 1m 回测并落库 CSV
# ---------------------------------------------------------------------------
import re as _re

_EXTRA_INDEX_CODES = [
    "000300.XSHG", "399101.XSHE", "399006.XSHE", "000510.XSHG",
]


def _extract_fixed_pools(strategy_text: str):
    """从策略源码中提取固定 ETF 池（全球池 + 中国池）。"""
    codes = _re.findall(r"\b(\d{6}\.(?:XSHG|XSHE))\b", strategy_text)
    seen = []
    for c in codes:
        if c not in seen:
            seen.append(c)
    return seen


def _ts_to_jq(ts_code: str) -> str:
    """tushare ts_code -> 聚宽 order_book_id（159059.SZ -> 159059.XSHE）。"""
    code, _, mkt = ts_code.partition(".")
    return "{}.{}".format(code, "XSHG" if mkt == "SH" else "XSHE")


# 基金公司前缀（与策略 FUND_COMPANIES 对齐，用于把 tushare 全称清洗成近似聚宽简称）
_FUND_COMPANIES = sorted(set([
    '易方达', '广发', '华夏', '华安', '嘉实', '富国', '招商', '鹏华', '南方', '汇添富', '国泰', '平安',
    '银华', '天弘', '建信', '工银', '华泰柏瑞', '博时', '景顺长城', '景顺', '华宝', '申万菱信', '万家', '中欧',
    '兴证全球', '浙商', '诺安', '前海开源', '泰康', '泰达宏利', '农银汇理', '交银', '东方红', '财通', '华商',
    '国联', '永赢', '金鹰', '德邦', '创金合信', '西部利得', '圆信永丰', '泓德', '汇安', '诺德', '恒生前海',
    '华润元大', '大成', '海富通', '摩根', '华泰', '中信', '中银', '兴全', '国信', '长城', '中金', '浙商证券',
    '东海', '东吴', '浦银安盛', '信达澳亚', '中加', '中航', '中融', '中邮', '中庚', '中信保诚', '中信建投',
    '中银国际', '中银证券', '九泰', '交银施罗德', '光大保德信', '兴银', '农银', '国投瑞银', '国海富兰克林',
    '国联安', '国金', '太平', '方正富邦', '民生加银', '汇丰晋信', '银河', '长信', '长安', '长盛', '长江证券', '鹏扬',
]), key=len, reverse=True)

# 指数编制机构词：聚宽 display_name 不含这些，但会导致 exclude 规则误杀行业 ETF，需清洗。
# 注意：宽基 ETF 清洗后仍保留 300/500/1000 等数字，会被策略 exclude 正确排除。
_INDEX_MAKERS = ['中证', '上证', '深证', '国证', '沪深', '中华', '中国']


def _clean_etf_name(name: str) -> str:
    """把 tushare ETF 全称清洗成近似聚宽 display_name 的简称。

    仅去除基金公司前缀与指数编制机构词（不动行业/主题/数字），使策略的
    exclude / 行业分组逻辑能像在聚宽 display_name 上一样工作。
    """
    if not name:
        return name
    s = name
    for c in _FUND_COMPANIES:
        s = s.replace(c, "")
    for m in _INDEX_MAKERS:
        s = s.replace(m, "")
    return s.strip()


# ETF 名录快照：同一策略回测结果可复现（避免每次启动实时拉取 tushare/mootdx
# 导致宇宙随时间漂移）。快照内容不含缓存并集（缓存并集在加载时并入）。
_ETF_UNIVERSE_SNAPSHOT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "jqengine", "data",
    "etf_universe_snapshot.json")
_ETF_SNAPSHOT_MAX_AGE = _dt.timedelta(days=7)


def _read_etf_snapshot(path):
    """读取 ETF 名录快照。返回 (fetched_at, codes, names, list_dates)；文件缺失/
    损坏/为空返回 None。list_dates 值恢复为 (list_date, delist_date) 元组。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            snap = json.load(f)
        codes = list(snap.get("codes") or [])
        if not codes:
            return None
        fetched_at = _dt.datetime.fromisoformat(str(snap["fetched_at"]))
        names = dict(snap.get("names") or {})
        list_dates = {k: tuple(v) for k, v in dict(snap.get("list_dates") or {}).items()}
        return fetched_at, codes, names, list_dates
    except Exception:  # noqa: BLE001
        return None


def _write_etf_snapshot(path, codes, names, list_dates):
    """原子写回 ETF 名录快照（先写临时文件再 rename，避免崩溃留下半截文件）。"""
    payload = {
        "fetched_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "codes": list(codes),
        "names": dict(names),
        "list_dates": {k: list(v) for k, v in list_dates.items()},
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, path)


def _merge_cache_daily_codes(dm, codes, names, tdx_names=None):
    """缓存并集：tushare fund_basic 名录不全（实测 589720.XSHG 等有日线数据
    却查不到 fund_basic 记录），若只按 fund_basic 建池，聚宽池里有、本地池
    缺失的标的会漏出候选（对账实测聚宽 04-09 首选 589720 本地缺席）。
    本地日线缓存中有数据的代码一并纳入宇宙；名称取 tdx_names 兜底为代码本身，
    list_date 无数据时退化为全程可交易（get_all_securities 的 _LIST_DATES 兜底）。
    """
    tdx_names = tdx_names or {}
    cache_codes = [k.split("get_daily_", 1)[1]
                   for k in (getattr(dm, "_daily_mem", None) or {})
                   if k.startswith("get_daily_")]
    have = set(codes)
    for c in cache_codes:
        if c not in have:
            codes.append(c)
            names.setdefault(c, tdx_names.get(c.split(".", 1)[0], c))
    return codes, names


def _fetch_etf_universe_online(dm):
    """实时拉取全市场 ETF 名录：tushare get_etf_list + mootdx 证券简称。

    返回 (codes, names, list_dates, tdx_names)；tushare 失败返回 None（mootdx
    失败容忍，回退 tushare 全称清洗）。名称口径：优先 mootdx（通达信）证券简称，
    与聚宽 get_security_name/display_name 一致（如 513350 -> "油气ETF"）。
    tushare fund_basic 只给全称（"标普石油天然气..."），其 name[:2] 分组键与
    聚宽不同，会导致行业去重选出不同代表 ETF。缺失时回退清洗全称。
    """
    try:
        src = dm.sources.get("tushare")
        rows = src.get_etf_list()
    except Exception as e:  # noqa: BLE001
        print("[universe] tushare get_etf_list 失败:", e)
        return None
    tdx_names = {}
    try:
        tdx_names = dm.sources["mootdx"].get_stock_names() or {}
    except Exception as e:  # noqa: BLE001
        print("[universe] mootdx get_stock_names 失败，回退 tushare 全称清洗:", e)

    def _fmt_date(v):
        """tushare YYYYMMDD -> 'YYYY-MM-DD'；非法/缺失返回 None。"""
        s = str(v or "").strip()
        if len(s) == 8 and s.isdigit():
            return "{}-{}-{}".format(s[:4], s[4:6], s[6:8])
        return None

    codes = []
    names = {}
    list_dates = {}
    for r in rows:
        try:
            jq = _ts_to_jq(r["ts_code"])
        except Exception:
            continue
        codes.append(jq)
        short = tdx_names.get(jq.split(".", 1)[0])
        names[jq] = short or _clean_etf_name(r.get("name", jq)) or jq
        ld = _fmt_date(r.get("list_date"))
        dd = _fmt_date(r.get("delist_date"))
        if ld or dd:
            list_dates[jq] = (ld or "2000-01-01", dd or "2999-12-31")
    return codes, names, list_dates, tdx_names


def _load_etf_universe(dm):
    """加载全市场 ETF 名录（代码 + 清洗后名称 + 上市/退市日期），对齐聚宽
    get_all_securities(['etf'])。返回 (codes, name_map, list_dates)，其中
    list_dates 为 {code: (list_date, delist_date)}（'YYYY-MM-DD'）。快照与
    网络都不可用时返回 ([], {}, {})，由调用方回退缓存派生宇宙。

    快照机制（保证同一策略结果可复现，不随每次启动的实时拉取漂移）：
    - 快照存在且 fetched_at 距今 ≤7 天：直接使用（离线/在线都优先）；
    - 快照缺失或过期且非离线模式：尝试网络刷新并原子写回快照；
    - 网络失败但快照存在：用旧快照并 warning；
    - dm._offline=True：只用快照或缓存派生，绝不联网（过期快照离线照用——
      无法刷新时旧名录仍优于无名录）。
    """
    snap = _read_etf_snapshot(_ETF_UNIVERSE_SNAPSHOT)
    fresh = (snap is not None
             and _dt.datetime.now() - snap[0] <= _ETF_SNAPSHOT_MAX_AGE)
    if fresh:
        codes, names = _merge_cache_daily_codes(dm, list(snap[1]), dict(snap[2]))
        return codes, names, snap[3]

    if not getattr(dm, "_offline", False):
        fetched = _fetch_etf_universe_online(dm)
        if fetched is not None:
            codes, names, list_dates, tdx_names = fetched
            try:
                _write_etf_snapshot(_ETF_UNIVERSE_SNAPSHOT, codes, names, list_dates)
            except Exception as e:  # noqa: BLE001
                logger.warning("[universe] ETF 名录快照写回失败（不影响本次回测）: %s", e)
            codes, names = _merge_cache_daily_codes(dm, codes, names, tdx_names)
            return codes, names, list_dates
        if snap is not None:
            logger.warning(
                "[universe] 网络刷新失败，回退旧 ETF 名录快照（fetched_at=%s）", snap[0])
            codes, names = _merge_cache_daily_codes(dm, list(snap[1]), dict(snap[2]))
            return codes, names, snap[3]
        print("[universe] tushare get_etf_list 失败且无快照，回退缓存派生")
        return [], {}, {}

    # 离线模式：绝不联网；快照（含过期）照用，无快照则回退缓存派生
    if snap is not None:
        codes, names = _merge_cache_daily_codes(dm, list(snap[1]), dict(snap[2]))
        return codes, names, snap[3]
    return [], {}, {}


def run_jq_backtest(strategy_path: str, params: dict,
                    universe=None, max_universe=None, db_path=None) -> dict:
    """运行聚宽式策略（如五福闹新春 v5.2）。

    注入 jqcompat 兼容层与 JqDataSource，设置 1m 频率回测，并将成交/净值写入
    ``runtime/jqwufu/trades.csv`` 与 ``equity.csv``。
    """
    if db_path:
        db.init_db(db_path)

    from app.quant.jqengine.datasource.manager import DataManager, get_data_manager
    from app.quant.jqengine.datasource.base import DataSourceError

    with open(strategy_path, "r", encoding="utf-8") as f:
        strategy_text = f.read()

    benchmark = params.get("benchmark", "510300.XSHG")
    start = (params.get("start") or "").strip() or "2026-01-01"
    end = (params.get("end") or "").strip() or "2026-07-08"

    # ---- 构造 DataManager，加载原始缓存（离线，无网络回源） ----
    # 使用单例，确保策略侧 get_data_manager() 拿到同一实例，避免 _use_real_minute
    # 等开关不一致（策略侧单例默认 True 会仍走 mootdx 实时分钟线网络）。
    dm = get_data_manager()
    # 确保 tushare token 生效（单例可能在 .env 加载前已创建，导致 token 为空）
    import os as _os
    _tok = _os.environ.get("TUSHARE_TOKEN") or CONFIG.get("TUSHARE_TOKEN", "")
    if _tok and dm.sources.get("tushare") is not None:
        try:
            dm.sources["tushare"].token = _tok
            import tushare as ts
            ts.set_token(_tok)
        except Exception:
            pass
    # 回测使用真实 1 分钟数据（real_ 基底），缺口由 baostock 5 分钟插值补齐。
    dm._use_real_minute = True
    # 名录快照必须在离线开关之前加载/刷新：dm._offline 只约束行情(bar)回源，
    # 而 _load_etf_universe 的快照刷新（tushare fund_basic + mootdx 名称，
    # ≤7 天一次的元数据调用）在离线模式下会被跳过、退化为"缓存派生宇宙+无
    # 名称"（实测池构建失真、结果不可复现）。先在线拿到快照，离线回测照用。
    try:
        _load_etf_universe(dm)
    except Exception:
        pass
    _prev_offline = getattr(dm, "_offline", False)
    dm._offline = True
    try:
        return _run_jq_backtest_inner(dm, strategy_text, params, benchmark, start, end,
                                      db_path, max_universe=max_universe,
                                      strategy_path=strategy_path)
    finally:
        # dm 是进程级单例：恢复 offline 开关，避免污染同进程后续调用方
        # （如实盘/策略侧需要联网回源的路径）。
        dm._offline = _prev_offline
        global _LIVE_RUN_ID
        _LIVE_RUN_ID = None
        try:
            import app.quant.jqcompat as _jqcompat_mod
            _jqcompat_mod.LIVE_SINK = None
        except Exception:  # noqa: BLE001
            pass


def _run_jq_backtest_inner(dm, strategy_text, params, benchmark, start, end, db_path,
                           max_universe=None, strategy_path=""):
    """run_jq_backtest 主体（独立成函数，便于上层用 try/finally 恢复 dm._offline）。"""
    from .jqcompat import install_jqcompat, JqDataSource

    dm.preload_daily()
    dm.set_minute_window(start, end)

    # 全量 ETF 宇宙（与聚宽 get_all_securities(['etf']) 对齐）：
    # 优先读 ETF 名录快照（≤7 天，离线/在线都优先，保证结果可复现）；快照缺失/
    # 过期且非离线时实时拉取 tushare+mootdx 并原子写回快照（按需回源：JqDataSource
    # 构造时对每只调 dm.fetch("get_daily")，本地缓存缺失即回源并落盘，无需独立
    # 预下载步骤）。快照与网络都不可用时回退到日线缓存键派生（离线可复现，此时
    # 无上市日期数据，get_all_securities(date=...) 退化为不过滤）。
    def _is_index(p):
        return p.startswith("000") or p.startswith("399")

    etf_universe, etf_names, etf_list_dates = _load_etf_universe(dm)
    if not etf_universe:
        all_codes = [k.split("get_daily_", 1)[1]
                     for k in dm._daily_mem if k.startswith("get_daily_")]
        etf_universe = [c for c in all_codes if not _is_index(c.split(".")[0])]
        etf_names = {}
        etf_list_dates = {}
    else:
        etf_universe = [c for c in etf_universe if not _is_index(c.split(".")[0])]
    if max_universe and len(etf_universe) > max_universe:
        etf_universe = etf_universe[:max_universe]
    print("[universe] 全市场 ETF 池: {} 只".format(len(etf_universe)))

    fixed_pools = _extract_fixed_pools(strategy_text)

    # 数据源宇宙需覆盖：全市场动态池(etf_universe) + 策略源码固定池(fixed_pools，
    # 含 LOF 如 501018 等不在 tushare ETF 列表中的标的) + 指数/基准/防御 ETF。
    # 固定池标的会被策略直接下单，必须建 instrument，否则 RQInvalidArgument。
    ds_universe = list(dict.fromkeys(
        list(etf_universe) + list(fixed_pools) + _EXTRA_INDEX_CODES
        + [benchmark, "511880.XSHG"]
    ))

    # 先构造数据源：包裹 DataManager，日线按需回源+展开 + 分钟线惰性加载。
    # （对 ds_universe 逐只 dm.fetch("get_daily")，本地缺失即回源落盘。）
    ds = JqDataSource(dm, ds_universe, start, end, benchmark=benchmark,
                      minute_cache_cap=int(params.get("minute_cache_cap", 800)))
    _set_pending_data_source(ds)

    # 惰性日线加载：不再预过滤 universe，JqDataSource 会按需加载日线并注册 instrument
    valid_universe = ds_universe
    print("[universe] 数据源覆盖标的: {} 只（含固定池+基准，惰性加载日线）".format(len(valid_universe)))

    # 安装兼容层（注册 shim + 补丁 + 假 jqdata；list_dates 供
    # get_all_securities(date=...) 按上市/退市日期过滤，避免幸存者偏差）
    install_jqcompat(valid_universe, names=etf_names, benchmark=benchmark,
                     list_dates=etf_list_dates)

    # 在 init/initialize 内注入 update_universe 与 _replay_run_daily，
    # 确保 1m 事件循环有标的、且全局 run_daily 在正确上下文内重放注册。
    # （聚宽允许全局调用 run_daily，rqalpha 要求在 init 上下文内注册，
    #  故 jqcompat 先缓存、init 时再重放。）
    universe_literal = ",\n".join('        "{}"'.format(c) for c in fixed_pools)
    inject = (
        "    update_universe([\n{}\n    ])"
    ).format(universe_literal)
    if _re.search(r"def initialize\s*\(", strategy_text):
        strategy_code = _re.sub(
            r"(def initialize\(context\):)",
            r"\1\n" + inject,
            strategy_text, count=1)
        # rqalpha 期望入口函数为 init（而非聚宽的 initialize），这里做别名桥接
        if not _re.search(r"def init\s*\(", strategy_text):
            strategy_code += "\ninit = initialize\n"
    elif _re.search(r"def init\s*\(", strategy_text):
        strategy_code = _re.sub(
            r"(def init\(context\):)",
            r"\1\n" + inject,
            strategy_text, count=1)
    else:
        strategy_code = strategy_text

    config = {
        "base": {
            "start_date": start,
            "end_date": end,
            "frequency": "1m",
            "run_type": "b",
            "accounts": {"stock": float(params.get("capital", 100000.0))},
            "benchmark": benchmark,
            "data_bundle_path": params.get("bundle_dir") or _dt_dir(),
            "matching_type": "current_bar",
            "strategy_file": "strategy.py",
        },
        "mod": {
            "sys_analyser": {"record": True, "benchmark": benchmark},
            "sys_simulation": {
                "slippage": float(params.get("slippage", 0.0001)),
                "matching_type": "current_bar",
                "price_limit": False,
                "volume_limit": False,
                "inactive_limit": False,
            },
            "sys_accounts": {
                "auto_switch_order_value": True,
            },
            "sys_transaction_cost": {
                # rqalpha 6.2.1 只认 stock_* 前缀键（旧键 commission_multiplier /
                # min_commission 为死键，会被忽略并退回默认万8 + 最低5元）
                "stock_commission_multiplier": float(params.get("fee", 0.0001)) / 0.0008,
                "stock_min_commission": float(params.get("min_commission", 5)),
            },
            "quantbridge": {"enabled": True},
            "jqbarcache": {"enabled": True},
        },
        "extra": {"log_level": params.get("log_level", "error")},
    }

    # 实时日志落库：聚宽策略的 log.info 经 jqcompat._Log -> LIVE_SINK 写 quant.db，
    # 否则前端日志标签始终为空。复用 rqalpha 路径的全局 run_id 与 sink 钩子。
    global _LIVE_RUN_ID
    run_id = params.get("run_id") or _re.sub(r"\W+", "", strategy_path).lower()[:16]
    _LIVE_RUN_ID = run_id
    try:
        import app.quant.jqcompat as _jqcompat_mod
        _jqcompat_mod.LIVE_SINK = (
            lambda level, msg: db.insert_log(run_id, _now(), level, msg)
            if _LIVE_RUN_ID is not None else None
        )
    except Exception:  # noqa: BLE001
        pass

    out_dir = params.get("out_dir") or os.path.join(CONFIG.runtime_dir, "jqwufu")
    os.makedirs(out_dir, exist_ok=True)

    try:
        from rqalpha import run as rq_run
        result = rq_run(config, source_code=strategy_code)

        equity = _extract_equity(result)
        trades = _extract_trades(result)

        eq_path = os.path.join(out_dir, "equity.csv")
        tr_path = os.path.join(out_dir, "trades.csv")
        # 列语义与 DB 一致：第 3 列为基准净值（benchmark），pnl/pnl_pct 为移动
        # 平均成本法已实现盈亏（见 _extract_equity/_extract_trades）
        pd.DataFrame(equity, columns=["date", "value", "benchmark", "cash", "market_value"]).to_csv(eq_path, index=False)
        pd.DataFrame(trades, columns=["dt", "code", "side", "price", "qty", "pnl", "pnl_pct", "cost"]).to_csv(tr_path, index=False)

        try:
            summary = result["sys_analyser"]["summary"]

            def _num(v):
                v = float(v if v is not None else 0.0)
                # NaN/Inf 不是合法 JSON，转 None 避免前端 JSON.parse 失败
                if v != v or v in (float("inf"), float("-inf")):
                    return None
                return v

            metrics = {
                "total_return": _num(summary.get("total_returns")),
                "annualized": _num(summary.get("annualized_returns")),
                "sharpe": _num(summary.get("sharpe")),
                "max_drawdown": _num(summary.get("max_drawdown")),
            }
        except Exception:
            metrics = {}

        n_trades = len(trades)
        final_equity = equity[-1][1] if equity else float(params.get("capital", 100000.0))

        if db_path:
            run_id = params.get("run_id") or _re.sub(r"\W+", "", strategy_path).lower()[:16]
            db.upsert_run(run_id, params.get("strategy_id", "wufu"), params.get("name", ""), json.dumps(params, ensure_ascii=False), "done")
            db.bulk_insert_equity(run_id, equity)
            for t in trades:
                db.insert_trade(run_id, *t)
            db.update_run(run_id, "done",
                          metrics_json=json.dumps(metrics, ensure_ascii=False),
                          finished_at=_now())

        return {
            "trades_csv": tr_path,
            "equity_csv": eq_path,
            "n_trades": n_trades,
            "final_equity": final_equity,
            "metrics": metrics,
            "universe_size": len(etf_universe),
        }
    except Exception as e:  # noqa: BLE001
        logger.exception("聚宽回测失败: %s", e)
        return {"error": str(e)}
