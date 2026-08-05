"""数据源聚合：本地分区 / mootdx / astock + 当日分钟内存库 + 共享网络拉取线程池。

内存策略（重要）：
- 本地分区已有的历史数据 → 每次按需读盘，**不常驻内存**（短 TTL 仅突发去重）；
- 本地没有、需网络拿的数据（当日实时分钟）→ 拿到后进**当日分钟内存库**，
  当日驻留，次日 00:00 清空，避免重复回源。
- 当日分钟内存库是纯 lazy dict：服务启动不预载、不预分配，未请求标的零内存。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

import polars as pl

from .single_flight import DedupCache, SingleFlight

logger = logging.getLogger("app.services.stockdata.sources")

_HIST_TTL = 60.0  # 历史日线/分钟短 TTL（仅突发去重，不驻留）

_T = TypeVar("_T")


def _tf_symbol(code: str) -> str:
    """平台代码(.XSHG/.XSHE/.SH/.SZ) -> 分区符号(.SH/.SZ)。"""
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".SH" if suf in ("XSHG", "SH") else ".SZ")


def _to_jq(code: str) -> str:
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".XSHG" if suf in ("SH", "XSHG") else ".XSHE")


def _is_index(code: str) -> bool:
    """指数判定：399 开头任意市场；000xxx 仅沪市（SH/XSHG）是指数。

    深市 000xxx（如 000001 平安银行）是股票，不能误走指数通道（mootdx 深市
    000xxx 走 index_bars 返回空）。同 mootdx_src._is_index。
    """
    pure = code.split(".", 1)[0]
    suffix = code.split(".", 1)[1] if "." in code else ""
    if pure.startswith("399"):
        return True
    return (suffix in ("SH", "XSHG") and pure.startswith("000")
            and len(pure) == 6 and not pure.startswith("0000"))


def _in_trading(now: _dt.datetime | None = None) -> bool:
    """交易时段判定（口径同 quant.simulate.runner.in_trading）。"""
    now = now or _dt.datetime.now()
    t = now.time()
    return (now.weekday() < 5
            and (_dt.time(9, 30) <= t <= _dt.time(11, 30)
                 or _dt.time(13, 0) <= t <= _dt.time(15, 0)))


def _normalize_etf_volume_unit(df: pl.DataFrame) -> pl.DataFrame:
    """ETF 日线 volume 归一为「股」（同 DataManager._normalize_etf_volume_unit）。"""
    if df is None or df.is_empty() or "volume" not in df.columns:
        return df
    ratio = (pl.col("amount") / (pl.col("volume") * pl.col("close"))).alias("_ratio")
    per_sym = df.group_by("symbol", maintain_order=True).agg(ratio.first())
    hand_syms = per_sym.filter(pl.col("_ratio") > 50).select("symbol")
    if hand_syms.is_empty():
        return df
    hand_set = set(hand_syms["symbol"].to_list())
    return df.with_columns(
        pl.when(pl.col("symbol").is_in(hand_set))
        .then(pl.col("volume") * 100)
        .otherwise(pl.col("volume"))
        .alias("volume")
    )


_MINUTE_COLS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


def _as_datetime(df: pl.DataFrame, col: str = "datetime") -> pl.DataFrame:
    """分区/内存帧的 datetime 统一为 Datetime 类型（落盘为 Datetime("us")）。"""
    if df.is_empty() or col not in df.columns or df.schema[col] != pl.Utf8:
        return df
    return df.with_columns(pl.col(col).str.to_datetime())


class MinuteMemoryStore:
    """当日分钟内存库：纯 lazy dict，只存「客户端请求过、经网络拉到」的当日实时分钟。

    不预载、不预分配；换日 lazy 清空（scheduler 在 00:00 主动清一次）。
    """

    def __init__(self) -> None:
        self._frames: dict[str, pl.DataFrame] = {}
        self._day: _dt.date | None = None
        self._lock = threading.Lock()

    def day(self) -> _dt.date | None:
        with self._lock:
            return self._day

    def ensure_day(self, day: _dt.date) -> None:
        """换日 lazy 清空：内存库只保留 `day` 当天的数据。"""
        with self._lock:
            if self._day != day:
                self._frames.clear()
                self._day = day

    def clear(self) -> None:
        with self._lock:
            self._frames.clear()
            self._day = None

    def update(self, day: str, frames: list[pl.DataFrame] | pl.DataFrame) -> None:
        """把网络拉到/当日分区的分钟帧并入内存库（same-day）。"""
        if isinstance(frames, pl.DataFrame):
            frames = [frames]
        if not frames:
            return
        with self._lock:
            self._day = _dt.datetime.fromisoformat(day).date()
            for df in frames:
                if df.is_empty():
                    continue
                df = _as_datetime(df)
                syms = set(df["symbol"].to_list())
                for sym in syms:
                    sub = df.filter(pl.col("symbol") == sym)
                    old = self._frames.get(sym)
                    merged = pl.concat([old, sub]).unique(
                        subset=["datetime"], keep="last").sort("datetime") if old is not None \
                        else sub
                    self._frames[sym] = merged

    def get_slice(self, symbols: set[str], lo_ts: str, hi_ts: str) -> pl.DataFrame:
        """取内存库中指定标的在 [lo_ts, hi_ts] 的当日分钟（空帧当无数据）。"""
        with self._lock:
            parts = [self._frames[s] for s in symbols if s in self._frames]
            if not parts:
                return pl.DataFrame(schema={c: pl.Utf8 for c in _MINUTE_COLS})
            df = pl.concat(parts).filter(
                (pl.col("datetime") >= pd_to_ts(lo_ts)) & (pl.col("datetime") <= pd_to_ts(hi_ts)))
            return df


class NetworkPuller:
    """服务端共享网络拉取线程池：有界并发 + 每线程独立数据源 + 标的级 single-flight。

    所有 handler 线程的实时回源都提交到此池：并发客户端请求的重叠标的内在
    ``rt:{code}`` 键上只拉一次，对 mootdx 的并发 socket 数被池上限约束。
    """

    def __init__(self, factory: Callable | None = None, workers: int = 16):
        self._factory = factory
        self._workers = max(1, workers)
        self._single = SingleFlight()
        self._local = threading.local()
        self._pool = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="stockdata-pull")

    def _source(self):
        src = getattr(self._local, "src", None)
        if src is None:
            if self._factory is not None:
                src = self._factory()
            else:
                from app.quant.jqengine.datasource.mootdx_src import MootdxSource
                src = MootdxSource()
            self._local.src = src
        return src

    def _fetch_one(self, code: str) -> pl.DataFrame:
        try:
            df = _pull_recent_guarded(self._source(), code)
        except TimeoutError:
            # 超时：复用 socket 可能已坏，重置本线程数据源，下次 fetch 重建
            self._local.src = None
            return pl.DataFrame()
        if df is None or df.empty:
            return pl.DataFrame()
        pdf = df.reset_index()
        pdf["symbol"] = _tf_symbol(code)
        for c in _MINUTE_COLS:
            if c not in pdf.columns:
                pdf[c] = None
        return _as_datetime(pl.from_pandas(pdf[_MINUTE_COLS]))

    def fetch_minute(self, code: str) -> pl.DataFrame:
        """单只标的实时分钟（per-symbol 去重：同一分钟多请求只回源一次）。"""
        return self._single.run(f"rt:{code}", lambda: self._fetch_one(code))

    def fetch_many(self, codes: list[str]) -> list[pl.DataFrame]:
        futures = {self._pool.submit(self.fetch_minute, c): c for c in codes}
        out = []
        for f in futures:
            try:
                df = f.result()
            except Exception as e:  # noqa: BLE001
                logger.warning("[sources] 实时回源异常 %s: %s", f, e)
                continue
            if df is not None and not df.is_empty():
                out.append(df)
        return out

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False)


def _pull_recent_guarded(src, code: str, timeout: float = 30.0):
    """墙钟守护的单只 mootdx 实时分钟拉取。

    超时抛 TimeoutError（调用方需重建数据源，避免复用可能已坏的非线程安全 socket）；
    异常/空帧返回 None。
    """
    import threading as _th
    box: dict = {}

    def _run():
        try:
            box["df"] = src.get_minute_recent(code, pages=1)
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    t = _th.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        logger.warning("[sources] %s 实时回源超时(%ss)，将重建数据源", code, timeout)
        raise TimeoutError(f"mootdx realtime pull timeout: {code}")
    if "err" in box:
        logger.warning("[sources] %s 实时回源失败: %s", code, box["err"])
        return None
    df = box.get("df")
    if df is None or df.empty:
        return None
    return df


class DataSources:
    """聚合源：本地分区读取为主 + 当日分钟内存库 + 共享网络拉取池。"""

    def __init__(self, data_root: str | None = None, mootdx_factory: Callable | None = None,
                 fetch_workers: int | None = None):
        self.data_root = data_root or os.getenv(
            "PARTITION_DATA_ROOT",
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "data"),
        )
        if fetch_workers is None:
            try:
                fetch_workers = int(os.getenv("STOCKDATA_FETCH_WORKERS", "") or 16)
            except (TypeError, ValueError):
                fetch_workers = 16
        self.dedup = DedupCache()
        self.minute_store = MinuteMemoryStore()
        self.puller = NetworkPuller(factory=mootdx_factory, workers=fetch_workers)

    # ---- 去重透传 ----
    def get_or_fetch(self, key: str, ttl: float, loader: Callable[[], _T]) -> _T:
        return self.dedup.get_or_fetch(key, ttl, loader)

    # ---- 分区扫描 ----
    def _scan_partitions(self, subdir: str, day_lo: str | None, day_hi: str | None,
                         symbols: set[str] | None, cols: list[str]) -> pl.DataFrame:
        root = os.path.join(self.data_root, subdir)
        if not os.path.isdir(root):
            return pl.DataFrame()
        paths = []
        for name in sorted(os.listdir(root)):
            if not name.startswith("date="):
                continue
            ds = name[len("date="):]
            if day_lo and ds < day_lo:
                continue
            if day_hi and ds > day_hi:
                continue
            import glob as _glob
            paths.extend(_glob.glob(os.path.join(root, name, "*.parquet")))
        if not paths:
            return pl.DataFrame()
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        if symbols:
            lf = lf.filter(pl.col("symbol").is_in(list(symbols)))
        out = lf.select(cols).collect()
        return _as_datetime(out)

    def _daily_days(self, lookback_days: int, asof: _dt.date | None) -> tuple[str | None, str | None]:
        end = asof or _dt.date.today()
        lo = end - _dt.timedelta(days=lookback_days * 2)  # 余量覆盖非交易日
        return lo.isoformat(), end.isoformat()

    def _load_daily(self, lookback_days: int, asof: _dt.date | None) -> pl.DataFrame:
        lo, hi = self._daily_days(lookback_days, asof)
        cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        parts = []
        # 批量预载只含股票+ETF：指数（kline_index_daily）会污染下游 ETF 宇宙
        # （_is_jq_etf_code 放行 932xxx 等指数代码，如 932000.XSHG）。指数日线
        # 仍走 get_daily 按需服务（策略 get_price 指数等），不入预载。
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False)):
            df = self._scan_partitions(subdir, lo, hi, None, cols)
            if df.is_empty():
                continue
            if is_stock:
                df = df.with_columns((pl.col("volume") * 100).alias("volume"))
            parts.append(df)
        if not parts:
            return pl.DataFrame()
        out = pl.concat(parts)
        out = _normalize_etf_volume_unit(out)
        if asof is not None:
            out = out.filter(pl.col("date") <= asof)
        return out

    def preload_daily(self, lookback_days: int = 400, asof: _dt.date | None = None) -> pl.DataFrame:
        key = f"preload_daily:{lookback_days}:{asof or ''}"
        return self.get_or_fetch(key, _HIST_TTL,
                                 lambda: self._load_daily(lookback_days, asof))

    def get_daily(self, codes: list[str], start_date: str, end_date: str) -> pl.DataFrame:
        def _load():
            syms = {_tf_symbol(c) for c in codes}
            cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
            parts = []
            for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False),
                                     ("kline_index_daily", False)):
                df = self._scan_partitions(subdir, start_date, end_date, syms, cols)
                if df.is_empty():
                    continue
                if is_stock:
                    df = df.with_columns((pl.col("volume") * 100).alias("volume"))
                parts.append(df)
            if not parts:
                return pl.DataFrame()
            return _normalize_etf_volume_unit(pl.concat(parts))

        # 短 TTL 仅突发去重，不驻留内存（历史数据每轮仍按需读盘）
        key = f"daily:{','.join(sorted(codes))}:{start_date}:{end_date}"
        return self.get_or_fetch(key, _HIST_TTL, _load)

    def get_minute(self, codes: list[str], lo_ts, hi_ts) -> pl.DataFrame:
        """历史分钟读分区；若请求范围包含今日，叠加当日分钟内存库（网络数据）。"""
        def _load():
            lo_d = str(pd_to_date(lo_ts)) if lo_ts is not None else None
            hi_d = str(pd_to_date(hi_ts)) if hi_ts is not None else None
            syms = {_tf_symbol(c) for c in codes}
            parts = []
            for subdir in ("kline_etf_minute", "kline_minute"):
                df = self._scan_partitions(subdir, lo_d, hi_d, syms, _MINUTE_COLS)
                if not df.is_empty():
                    parts.append(df)
            today = _dt.date.today()
            if (lo_d is None or lo_d <= today.isoformat()) and (hi_d is None or hi_d >= today.isoformat()):
                mem = self.minute_store.get_slice(syms, str(lo_ts or today), str(hi_ts or f"{today} 15:00:00"))
                if not mem.is_empty():
                    parts.append(mem)
            if not parts:
                return pl.DataFrame()
            out = pl.concat(parts).unique(subset=["symbol", "datetime"], keep="last")
            if lo_ts is not None:
                out = out.filter(pl.col("datetime") >= pd_to_ts(lo_ts))
            if hi_ts is not None:
                out = out.filter(pl.col("datetime") <= pd_to_ts(hi_ts))
            return out

        # 短 TTL 仅突发去重；10s 保证当日内存库叠加层不过期
        key = f"min:{','.join(sorted(codes))}:{lo_ts}:{hi_ts}"
        return self.get_or_fetch(key, 10.0, _load)

    def get_realtime_snapshot(self, codes: list[str], as_of=None) -> pl.DataFrame:
        """当日分钟内存库 + 未覆盖标的共享拉取池按需补实时（per-symbol 去重）。

        实时回源只在交易时段执行；非交易时段只读内存库 + 当日分区（不触网）。
        指数标的（仅用日线）不参与实时回源。
        """
        asof_ts = pd_to_ts(as_of) if as_of is not None else _dt.datetime.now()
        today = asof_ts.date()
        tf_syms = {_tf_symbol(c) for c in codes}
        self.minute_store.ensure_day(today)

        # 基础帧：当日分区（收盘同步/重启场景）+ 内存库（网络实时）
        base_parts = []
        part = self._scan_partitions("kline_etf_minute", today.isoformat(),
                                     today.isoformat(), tf_syms, _MINUTE_COLS)
        if not part.is_empty():
            base_parts.append(part)
        mem = self.minute_store.get_slice(tf_syms, f"{today} 00:00:00", str(asof_ts))
        if not mem.is_empty():
            base_parts.append(mem)
        base = pl.concat(base_parts).unique(subset=["symbol", "datetime"], keep="last") \
            if base_parts else pl.DataFrame(schema={c: pl.Utf8 for c in _MINUTE_COLS})

        # 未覆盖：内存缺失，或内存最新 bar < asof（过期）。指数跳过。非交易时段不拉。
        latest_by_sym = {}
        for sym, mx in base.group_by("symbol").agg(pl.col("datetime").max()).iter_rows():
            latest_by_sym[sym] = mx
        todo = [c for c in codes
                if _in_trading(asof_ts) and not _is_index(c)
                and (_tf_symbol(c) not in latest_by_sym
                     or latest_by_sym[_tf_symbol(c)] < asof_ts - _dt.timedelta(minutes=3))]
        fills: list[pl.DataFrame] = []
        if todo:
            pulls = self.puller.fetch_many(todo)
            for df in pulls:
                if not df.is_empty():
                    fills.append(df)
            # 更新当日分钟内存库（网络数据才驻留）
            if fills:
                self.minute_store.update(today.isoformat(), fills)

        if not (base_parts or fills):
            # 无基础帧也无实时填充：返回空帧、跳过过滤（空帧 datetime 为 Utf8，
            # 不能与 datetime 字面量比较 → InvalidOperationError）
            return pl.DataFrame(schema={c: pl.Utf8 for c in _MINUTE_COLS})
        out = pl.concat(base_parts + fills).unique(subset=["symbol", "datetime"], keep="last")
        out = out.filter(pl.col("datetime") <= asof_ts)
        return out.sort(["symbol", "datetime"])

    # ---- 元数据 ----
    def get_trade_days(self, start_date: str, end_date: str) -> list[str]:
        # 交易日历：从 kline_index_daily 分区索引推（沪深300 恒有数据）
        df = self._scan_partitions("kline_index_daily", start_date, end_date, None,
                                   ["date"]).unique(subset=["date"])
        return sorted(str(d) for d in df["date"].to_list())

    def get_all_securities(self, types: list[str] | None, date: str | None) -> pl.DataFrame:
        # 分区仅落 symbol/OHLCV（无 name/list_date）：只选 symbol，其余列以空值补齐
        # 保证客户端 schema 稳定
        parts = []
        if types is None or "stock" in types:
            df = self._scan_partitions("kline_daily", None, None, None, ["symbol"]) if os.path.isdir(
                os.path.join(self.data_root, "kline_daily")) else pl.DataFrame()
            if not df.is_empty():
                parts.append(df.unique(subset=["symbol"])
                              .with_columns(pl.lit("stock").alias("type")))
        if types is None or "etf" in types:
            df = self._scan_partitions("kline_etf_daily", None, None, None, ["symbol"]) if os.path.isdir(
                os.path.join(self.data_root, "kline_etf_daily")) else pl.DataFrame()
            if not df.is_empty():
                parts.append(df.unique(subset=["symbol"])
                              .with_columns(pl.lit("etf").alias("type")))
        if types is None or "index" in types:
            df = self._scan_partitions("kline_index_daily", None, None, None, ["symbol"]) if os.path.isdir(
                os.path.join(self.data_root, "kline_index_daily")) else pl.DataFrame()
            if not df.is_empty():
                parts.append(df.unique(subset=["symbol"])
                              .with_columns(pl.lit("index").alias("type")))
        if not parts:
            return pl.DataFrame(schema={"symbol": pl.Utf8, "name": pl.Utf8,
                                        "list_date": pl.Utf8, "type": pl.Utf8})
        return pl.concat(parts).with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("name"),
            pl.lit(None, dtype=pl.Utf8).alias("list_date"))

    def get_security_info(self, code: str) -> dict:
        df = self.get_all_securities(None, None)
        sym = _tf_symbol(code)
        row = df.filter(pl.col("symbol") == sym)
        if row.is_empty():
            return {}
        r = row.to_dicts()[0]
        return {"code": code, "name": r.get("name"), "type": r.get("type"),
                "start_date": r.get("list_date"), "end_date": None}

    def get_index_stocks(self, index_code: str, date: str | None) -> list[str]:
        # 成分股暂以全市场股票日线标的近似（不维护成分表）；有成分表后替换
        df = self._scan_partitions("kline_daily", None, None, None, ["symbol"])
        return sorted(set(df["symbol"].to_list()))

    def get_stock_names(self, codes: list[str] | None = None) -> dict:
        # 股票名称分区暂未落盘（partition 仅 symbol/OHLCV）；名称属展示层，空降级
        return {}

    def get_adj_factors(self) -> pl.DataFrame:
        root = os.path.join(self.data_root, "adj_factor_etf")
        if not os.path.isdir(root):
            return pl.DataFrame()
        import glob as _glob
        paths = _glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True)
        if not paths:
            return pl.DataFrame()
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        cols = lf.columns
        if "symbol" not in cols:
            lf = lf.with_columns(pl.lit("").alias("symbol"))
        return lf.select(["symbol", "trade_date", "ex_factor"]).collect()


def pd_to_ts(x):
    import pandas as pd
    return pd.Timestamp(x)


def pd_to_date(x):
    import pandas as pd
    return pd.Timestamp(x).date()
