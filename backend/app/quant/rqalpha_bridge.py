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

from rqalpha.const import INSTRUMENT_TYPE, MARKET, TRADING_CALENDAR_TYPE
from rqalpha.interface import ExchangeRate
from rqalpha.model.instrument import Instrument

from . import db
from .config import CONFIG, QuantConfig

logger = logging.getLogger(__name__)


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

        for code in symbols:
            df = provider.get_daily(code, start, end)
            if df is None or len(df) == 0:
                continue
            self._bars[code] = self._df_to_recarray(df)
            self._instruments[code] = self._make_instrument(code)
            for d in df["date"]:
                all_dates.add(pd.Timestamp(d).date())

        self._trading_dates = sorted(all_dates)

        # 注册 store（DataProxy 通过这些 store 取数）
        self._day_bar_stores = {}
        self._calendar_stores = {}
        self.register_day_bar_store(
            INSTRUMENT_TYPE.CS, _DayBarStore(self._bars), market=MARKET.CN
        )
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
    def _df_to_recarray(df: pd.DataFrame):
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
        arr["limit_up"] = df["high"].to_numpy(dtype=np.float64) * 1.1
        arr["limit_down"] = df["low"].to_numpy(dtype=np.float64) * 0.9
        return arr

    def _make_instrument(self, code: str) -> Instrument:
        return Instrument(
            {
                "order_book_id": code,
                "symbol": code.split(".")[0],
                "type": "CS",
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
        return self._trading_dates[0], self._trading_dates[-1]

    def get_yield_curve(self, start_date, end_date, tenor=None):
        return None

    def is_suspended(self, order_book_id, dates):
        return [False] * len(dates)

    def is_st_stock(self, order_book_id, dates):
        return [False] * len(dates)

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
        i = bars["datetime"].searchsorted(np.uint64(int(pd.Timestamp(dt).strftime("%Y%m%d"))), side="right")
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
        from app.quant.jqcompat import _register_jq_apis

        class _QuantBridgeMod(AbstractMod):
            def start_up(self, env, mod_config):
                # 系统 mod（sys_accounts/sys_simulation 等）已在各自 start_up
                # 用原生实现覆盖了 api 同名函数，这里兜底重新注册我们的 shim，
                # 确保策略 `from jqdata import *` 拿到的是兼容层实现。
                _register_jq_apis()
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
        return {
            "total_return": float(summary.get("total_returns") or 0.0),
            "annualized": float(summary.get("annualized_returns") or 0.0),
            "sharpe": float(summary.get("sharpe") or 0.0),
            "max_drawdown": float(summary.get("max_drawdown") or 0.0),
        }
    except Exception:
        return {}


def _extract_equity(result):
    try:
        pf = result["sys_analyser"]["portfolio"]
        out = []
        for d, row in pf.iterrows():
            dt = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
            out.append((
                dt,
                float(row.get("total_value", 0.0) or 0.0),
                float(row.get("unit_net_value", 1.0) or 1.0),
                float(row.get("cash", 0.0) or 0.0),
                float(row.get("market_value", 0.0) or 0.0),
            ))
        return out
    except Exception:
        return []


def _extract_trades(result):
    out = []
    try:
        trades = result["sys_analyser"]["trades"]
        for _, t in trades.iterrows():
            dt = t["datetime"]
            ts = dt.strftime("%Y-%m-%d %H:%M") if hasattr(dt, "strftime") else str(dt)
            out.append((
                ts,
                str(t["order_book_id"]),
                str(t["side"]),
                float(t.get("last_price", 0.0) or 0.0),
                float(t.get("last_quantity", 0.0) or 0.0),
                0.0,
                0.0,
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
            "frequency": "1d",
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
                "commission_multiplier": float(params.get("fee", 0.0003)) / 0.0008,
            },
            "quantbridge": {"enabled": True},
        },
        "extra": {"log_level": "error"},
    }


def _dt_dir() -> str:
    import tempfile

    d = os.path.join(tempfile.gettempdir(), "rqalpha_bridge_bundle")
    os.makedirs(d, exist_ok=True)
    return d


def run_backtest(strategy_code: str, params: dict, provider=None, db_path: str | None = None) -> dict:
    """跑回测并写 quant.db。返回 {run_id, metrics}。"""
    if db_path:
        db.init_db(db_path)
    run_id = params.get("run_id") or uuid.uuid4().hex[:8]
    db.upsert_run(run_id, params.get("strategy_id", ""), json.dumps(params, ensure_ascii=False), "running")
    try:
        from rqalpha import run as rq_run

        if provider is None:
            provider = _BundleProvider(params["bundle_dir"])
        ds = QuantRQAlphaDataSource(provider, CONFIG, params)
        _set_pending_data_source(ds)

        config = _build_config(params)
        result = rq_run(config, source_code=strategy_code)

        metrics = _extract_metrics(result)
        equity = _extract_equity(result)
        trades = _extract_trades(result)

        db.bulk_insert_equity(run_id, equity)
        for t in trades:
            db.insert_trade(run_id, *t)
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


def _load_etf_universe(dm):
    """从 tushare 拉取全市场 ETF 列表（代码 + 清洗后名称），对齐聚宽
    get_all_securities(['etf'])。返回 (codes, name_map)。失败返回 ([], {})。
    """
    try:
        src = dm.sources.get("tushare")
        rows = src.get_etf_list()
    except Exception as e:  # noqa: BLE001
        print("[universe] tushare get_etf_list 失败，回退缓存派生:", e)
        return [], {}
    # 优先使用 mootdx（通达信）证券简称，与聚宽 get_security_name/display_name 一致
    # （如 513350 -> "油气ETF"）。tushare fund_basic 只给全称（"标普石油天然气..."），
    # 其 name[:2] 分组键与聚宽不同，会导致行业去重选出不同代表 ETF。缺失时回退清洗全称。
    tdx_names = {}
    try:
        tdx_names = dm.sources["mootdx"].get_stock_names() or {}
    except Exception as e:  # noqa: BLE001
        print("[universe] mootdx get_stock_names 失败，回退 tushare 全称清洗:", e)
    codes = []
    names = {}
    for r in rows:
        try:
            jq = _ts_to_jq(r["ts_code"])
        except Exception:
            continue
        codes.append(jq)
        short = tdx_names.get(jq.split(".", 1)[0])
        names[jq] = short or _clean_etf_name(r.get("name", jq)) or jq
    return codes, names


def run_jq_backtest(strategy_path: str, params: dict,
                    universe=None, max_universe=None, db_path=None) -> dict:
    """运行聚宽式策略（如五福闹新春 v5.2）。

    注入 jqcompat 兼容层与 JqDataSource，设置 1m 频率回测，并将成交/净值写入
    ``runtime/jqwufu/trades.csv`` 与 ``equity.csv``。
    """
    if db_path:
        db.init_db(db_path)

    from .jqcompat import install_jqcompat, JqDataSource
    from app.quant.jqengine.datasource.manager import DataManager, get_data_manager
    from app.quant.jqengine.datasource.base import DataSourceError

    with open(strategy_path, "r", encoding="utf-8") as f:
        strategy_text = f.read()

    benchmark = params.get("benchmark", "510300.XSHG")
    start = params.get("start", "2026-01-01")
    end = params.get("end", "2026-07-08")

    # ---- 构造 DataManager，加载原始缓存（离线，无网络回源） ----
    # 使用单例，确保策略侧 get_data_manager() 拿到同一实例，避免 _use_real_minute
    # 等开关不一致（策略侧单例默认 True 会仍走 mootdx 实时分钟线网络）。
    dm = get_data_manager()
    dm._use_real_minute = True  # 回测保留 mootdx 实时分钟线（本地缓存优先，保证收益对齐基线）
    dm.preload_daily()
    dm.set_minute_window(start, end)

    # 全量 ETF 宇宙（与聚宽 get_all_securities(['etf']) 对齐）：
    # 优先从 tushare 拉取全市场 ETF 列表（按需回源：JqDataSource 构造时对每只
    # 调 dm.fetch("get_daily")，本地缓存缺失即回源并落盘，无需独立预下载步骤）。
    # tushare 不可用时回退到日线缓存键派生（离线可复现）。
    def _is_index(p):
        return p.startswith("000") or p.startswith("399")

    etf_universe, etf_names = _load_etf_universe(dm)
    if not etf_universe:
        all_codes = [k.split("get_daily_", 1)[1]
                     for k in dm._daily_mem if k.startswith("get_daily_")]
        etf_universe = [c for c in all_codes if not _is_index(c.split(".")[0])]
        etf_names = {}
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

    # 安装兼容层（注册 shim + 补丁 + 假 jqdata）
    install_jqcompat(valid_universe, names=etf_names, benchmark=benchmark)

    # 在 init/initialize 内注入 update_universe 与 _replay_run_daily，
    # 确保 1m 事件循环有标的、且全局 run_daily 在正确上下文内重放注册。
    # （聚宽允许全局调用 run_daily，rqalpha 要求在 init 上下文内注册，
    #  故 jqcompat 先缓存、init 时再重放。）
    universe_literal = ",\n".join('        "{}"'.format(c) for c in fixed_pools)
    inject = (
        "    from app.quant.jqcompat import _replay_run_daily\n"
        "    _replay_run_daily()\n"
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
                "commission_multiplier": float(params.get("fee", 0.0001)) / 0.0008,
                "min_commission": 5,
            },
            "quantbridge": {"enabled": True},
            "jqbarcache": {"enabled": True},
        },
        "extra": {"log_level": params.get("log_level", "error")},
    }

    out_dir = params.get("out_dir") or os.path.join(CONFIG.runtime_dir, "jqwufu")
    os.makedirs(out_dir, exist_ok=True)

    try:
        from rqalpha import run as rq_run
        result = rq_run(config, source_code=strategy_code)

        equity = _extract_equity(result)
        trades = _extract_trades(result)

        eq_path = os.path.join(out_dir, "equity.csv")
        tr_path = os.path.join(out_dir, "trades.csv")
        pd.DataFrame(equity, columns=["date", "value", "unit_net_value", "cash", "market_value"]).to_csv(eq_path, index=False)
        pd.DataFrame(trades, columns=["dt", "code", "side", "price", "qty", "a", "b", "cost"]).to_csv(tr_path, index=False)

        try:
            summary = result["sys_analyser"]["summary"]
            metrics = {
                "total_return": float(summary.get("total_returns") or 0.0),
                "annualized": float(summary.get("annualized_returns") or 0.0),
                "sharpe": float(summary.get("sharpe") or 0.0),
                "max_drawdown": float(summary.get("max_drawdown") or 0.0),
            }
        except Exception:
            metrics = {}

        n_trades = len(trades)
        final_equity = equity[-1][1] if equity else float(params.get("capital", 100000.0))

        if db_path:
            run_id = params.get("run_id") or _re.sub(r"\W+", "", strategy_path).lower()[:16]
            db.upsert_run(run_id, params.get("strategy_id", "wufu"), json.dumps(params, ensure_ascii=False), "done")
            db.bulk_insert_equity(run_id, equity)
            for t in trades:
                db.insert_trade(run_id, *t)

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
