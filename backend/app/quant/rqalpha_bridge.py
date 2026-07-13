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

        class _QuantBridgeMod(AbstractMod):
            def start_up(self, env, mod_config):
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
