"""数据源聚合：本地分区 / mootdx / astock + 当日分钟内存库 + 共享网络拉取线程池。

内存策略（重要）：
- 本地分区已有的历史数据 → 每次按需读盘，**不常驻内存**（短 TTL 仅突发去重）；
- 本地没有、需网络拿的数据（当日实时分钟）→ 拿到后进**当日分钟内存库**，
  当日驻留，次日 00:00 清空，避免重复回源。
- 当日分钟内存库是纯 lazy dict：服务启动不预载、不预分配，未请求标的零内存。
"""
from __future__ import annotations

import datetime as _dt
import json as _json
import logging
import os
import threading
import time
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
    return pure + (".XSHG" if suf in ("SH", "SS", "XSHG") else ".XSHE")


def _is_index(code: str) -> bool:
    """指数判定：399 开头任意市场；000xxx 仅沪市（SH/SS/XSHG）是指数。

    深市 000xxx（如 000001 平安银行）是股票，不能误走指数通道（mootdx 深市
    000xxx 走 index_bars 返回空）。同 mootdx_src._is_index。
    """
    pure = code.split(".", 1)[0]
    suffix = code.split(".", 1)[1] if "." in code else ""
    if pure.startswith("399"):
        return True
    return (suffix in ("SH", "SS", "XSHG") and pure.startswith("000")
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
    """分区/内存帧的 datetime 统一为 Datetime("us")（与落盘分区口径一致）。

    仅 Utf8 转换不够：实时回源经 pl.from_pandas 得到 Datetime("ns")，分区 parquet
    为 Datetime("us")，两者 pl.concat 会因单位不一致抛 SchemaError。故对已存在的
    Datetime 列也统一 cast 到 us。
    """
    if df.is_empty() or col not in df.columns:
        return df
    dtype = df.schema[col]
    if dtype == pl.Utf8:
        return df.with_columns(pl.col(col).str.to_datetime())
    if isinstance(dtype, pl.Datetime) and dtype.time_unit != "us":
        return df.with_columns(pl.col(col).cast(pl.Datetime("us", dtype.time_zone)))
    return df


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


class DayFileCache:
    """日线日期文件缓存：键=(subdir, date) → 该日全市场整帧（原始单位）。

    日线分区按日存储、每日期文件含全市场标的：读取时整文件载入内存，同文件
    其他标的的后续请求直接命中。后台清扫线程每 10s 卸载超时（默认 60s）未
    访问的文件，并执行容量上限（默认 60 文件）淘汰。不预载、不驻留 400 天
    全市场整帧（spec 2026-08-21-stockdata-daily-dayfile-lru-design）。
    """

    def __init__(self, ttl: float = 60.0, cap: int = 60) -> None:
        self._ttl = ttl
        self._cap = cap
        self._items: dict[tuple[str, str], tuple[float, pl.DataFrame]] = {}
        self._lock = threading.Lock()
        self._single = SingleFlight()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def get(self, subdir: str, date: str) -> pl.DataFrame | None:
        """命中返回帧并刷新该文件最后访问时间；未命中返回 None（不加载）。"""
        with self._lock:
            item = self._items.get((subdir, date))
            if item is None:
                return None
            _ts, frame = item
            self._items[(subdir, date)] = (time.monotonic(), frame)
            return frame

    def get_or_load(self, subdir: str, date: str,
                    loader: Callable[[], pl.DataFrame | None]) -> pl.DataFrame | None:
        """缓存命中直接返回；未命中日期文件级 single-flight 读盘（同键并发只读一次）。"""
        hit = self.get(subdir, date)
        if hit is not None:
            return hit
        return self._single.run(
            f"{subdir}:{date}",
            lambda: self._insert(subdir, date, loader()))

    def _insert(self, subdir: str, date: str,
                frame: pl.DataFrame | None) -> pl.DataFrame | None:
        if frame is None or frame.is_empty():
            return None
        with self._lock:
            # double-check：single-flight 期间可能已有其他线程载入
            if (subdir, date) not in self._items:
                self._items[(subdir, date)] = (time.monotonic(), frame)
        return frame

    def sweep(self) -> int:
        """卸载超时未访问文件；仍超容量上限时按最后访问时间从旧到新踢。返回卸载数。"""
        now = time.monotonic()
        evicted = 0
        with self._lock:
            for k in [k for k, (ts, _f) in self._items.items() if now - ts > self._ttl]:
                del self._items[k]
                evicted += 1
            if len(self._items) > self._cap:
                oldest = sorted(self._items.items(), key=lambda kv: kv[1][0])
                for k, _v in oldest[: len(self._items) - self._cap]:
                    del self._items[k]
                    evicted += 1
        return evicted


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
        self.dayfile_cache = DayFileCache()
        self.puller = NetworkPuller(factory=mootdx_factory, workers=fetch_workers)
        self._names_map: dict[str, str] | None = None
        self._names_cache_file = os.path.join(self.data_root, ".stock_names_cache.json")
        self._index_pool_cache: dict[str, tuple[float, list[str]]] = {}

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
        out = lf.select(cols).collect(engine="streaming")
        return _as_datetime(out)

    def _daily_days(self, lookback_days: int, asof: _dt.date | None) -> tuple[str | None, str | None]:
        end = asof or _dt.date.today()
        lo = end - _dt.timedelta(days=lookback_days * 2)  # 余量覆盖非交易日
        return lo.isoformat(), end.isoformat()

    def _read_day_file(self, subdir: str, date: str,
                       cols: list[str] | None = None) -> pl.DataFrame | None:
        """读单个日期分区（含全市场标的）→ 原始帧；分区不存在返回 None。

        cols 缺省为日线 8 列；分钟分区传 _MINUTE_COLS。
        """
        root = os.path.join(self.data_root, subdir, f"date={date}")
        if not os.path.isdir(root):
            return None
        import glob as _glob
        paths = _glob.glob(os.path.join(root, "*.parquet"))
        if not paths:
            return None
        if cols is None:
            cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        return _as_datetime(lf.select(cols).collect())

    def _existing_day_files(self, subdir: str, lo: str | None,
                            hi: str | None) -> list[str]:
        """区间内已存在的日期分区名（升序，ISO 字符串）。"""
        root = os.path.join(self.data_root, subdir)
        if not os.path.isdir(root):
            return []
        out = []
        for name in sorted(os.listdir(root)):
            if not name.startswith("date="):
                continue
            ds = name[len("date="):]
            if lo and ds < lo:
                continue
            if hi and ds > hi:
                continue
            out.append(ds)
        return out

    def preload_daily(self, lookback_days: int = 400, asof: _dt.date | None = None) -> pl.DataFrame:
        """预载全市场日线（只含股票+ETF，不含指数）：逐日文件经 LRU 拼帧返回。

        帧不驻留（LRU 按 60s/60 文件自然淘汰）——spec
        2026-08-21-stockdata-daily-dayfile-lru-design 第 3 节。
        """
        lo, hi = self._daily_days(lookback_days, asof)
        parts = []
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False)):
            for day in self._existing_day_files(subdir, lo, hi):
                frame = self.dayfile_cache.get_or_load(
                    subdir, day, lambda s=subdir, d=day: self._read_day_file(s, d))
                if frame is None or frame.is_empty():
                    continue
                if is_stock:
                    frame = frame.with_columns((pl.col("volume") * 100).alias("volume"))
                parts.append(frame)
        if not parts:
            return pl.DataFrame()
        out = _normalize_etf_volume_unit(pl.concat(parts))
        if asof is not None:
            out = out.filter(pl.col("date") <= asof)
        return out

    def get_daily(self, codes: list[str], start_date: str, end_date: str) -> pl.DataFrame:
        # 日期规范化：兼容 %Y%m%d（模拟盘 jqcompat _DayBarStore 传入）与 ISO
        # （rqalpha_bridge 传入）两种格式。分区名恒为 ISO（date=YYYY-MM-DD），
        # 字符串比较；'20260601' 与 '2026-06-01' 比较恒 False 会把全部分区跳过。
        # 统一转 ISO 再比较。
        start_date = str(pd_to_date(start_date)) if start_date else None
        end_date = str(pd_to_date(end_date)) if end_date else None

        syms = {_tf_symbol(c) for c in codes}
        parts = []
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False),
                                 ("kline_index_daily", False)):
            for day in self._existing_day_files(subdir, start_date, end_date):
                frame = self.dayfile_cache.get_or_load(
                    subdir, day, lambda s=subdir, d=day: self._read_day_file(s, d))
                if frame is None or frame.is_empty():
                    continue
                sub = frame.filter(pl.col("symbol").is_in(syms))
                if sub.is_empty():
                    continue
                if is_stock:
                    sub = sub.with_columns((pl.col("volume") * 100).alias("volume"))
                parts.append(sub)
        if not parts:
            return pl.DataFrame()
        return _normalize_etf_volume_unit(pl.concat(parts))

    def get_etf_nav(self, codes: list[str], date: str | None = None) -> pl.DataFrame:
        """读 etf_nav 分区（date 给定用该日，None 用最新分区）。"""
        def _load():
            syms = {_to_jq(c) for c in codes}
            cols = ["symbol", "unit_nav", "date"]
            lo = hi = date
            if date is None:
                parts_root = os.path.join(self.data_root, "etf_nav")
                dates = sorted(
                    d[5:] for d in os.listdir(parts_root)
                    if d.startswith("date=")) if os.path.isdir(parts_root) else []
                if not dates:
                    return pl.DataFrame()
                hi = dates[-1]
            return self._scan_partitions("etf_nav", lo, hi, syms, cols)
        key = f"nav:{','.join(sorted(codes))}:{date or 'latest'}"
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

        # 基础帧：当日分区（收盘同步/重启场景，经日期文件 LRU 缓存避免逐请求重扫）
        # + 内存库（网络实时）。spec 2026-08-21-stockdata-realtime-cache-design。
        base_parts = []
        part = self.dayfile_cache.get_or_load(
            "kline_etf_minute", today.isoformat(),
            lambda: self._read_day_file("kline_etf_minute", today.isoformat(), _MINUTE_COLS))
        if part is not None and not part.is_empty():
            base_parts.append(part.filter(pl.col("symbol").is_in(tf_syms)))
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
        sym = _tf_symbol(code)
        info = self.get_security_infos([code]).get(sym)
        if info is None:
            # instruments 快照未覆盖（如仅 OHLCV 分区）：退回分区名录，字段可空
            df = self.get_all_securities(None, None)
            row = df.filter(pl.col("symbol") == sym)
            if row.is_empty():
                return {}
            r = row.to_dicts()[0]
            return {"code": code, "name": r.get("name"), "type": r.get("type"),
                    "start_date": r.get("list_date"), "end_date": None}
        return {"code": code, "name": info.get("name"), "type": info.get("type"),
                "start_date": info.get("start_date"), "end_date": None}

    def get_security_infos(self, codes=None) -> dict:
        """批量元数据：{symbol: {name, start_date, type}}。

        数据源：本地 instruments 快照（含 listing_date）。codes 为空返回全部。
        进程内缓存（instruments 为日级快照，TTL 600s 足够）。
        """
        def _load():
            import polars as _pl
            p = os.path.join(self.data_root, "instruments", "instruments.parquet")
            if not os.path.exists(p):
                return {}
            df = _pl.read_parquet(p)
            out = {}
            for r in df.iter_rows(named=True):
                sym = str(r.get("symbol") or "")
                if not sym:
                    continue
                ld = r.get("listing_date")
                out[sym] = {
                    "name": r.get("name"),
                    "start_date": str(ld) if ld else None,
                    "type": r.get("type"),
                }
            return out

        allmap = self.get_or_fetch("security_infos", 600.0, _load)
        if not codes:
            return dict(allmap)
        want = {_tf_symbol(str(c)) for c in codes}
        return {k: v for k, v in allmap.items() if k in want}

    def get_index_stocks(self, index_code: str, date: str | None) -> list[str]:
        """真实指数成分（当前成员快照；date 参数暂忽略，成分历史不入库）。

        来源：沪深300/上证50/中证500 → baostock 官方接口；其余（含国证系
        399xxx，如 399101 中小综指）→ 国证官网 sample-detail 接口。
        结果落 ``data/pools/<code6>.json`` 磁盘缓存；网络失败时回退最近一次
        快照（宁可陈旧也不返回全市场假成分）。
        """
        import json as _json
        code6 = str(index_code).split(".")[0].strip()
        pool_file = os.path.join(self.data_root, "pools", f"{code6}.json")

        def _read_pool():
            try:
                with open(pool_file, encoding="utf-8") as f:
                    return [str(s) for s in (_json.load(f).get("stocks") or [])]
            except Exception:
                return []

        now = time.time()
        hit = self._index_pool_cache.get(code6)
        if hit and now - hit[0] < 86400.0:
            return list(hit[1])
        stocks = self._fetch_index_stocks_live(code6)
        if not stocks:
            # 网络失败 → 最近磁盘快照兜底（宁可陈旧也不返回全市场假成分）
            stocks = _read_pool()
            if stocks:
                logger.warning("index_stocks %s 网络失败，回退磁盘快照 %d 只",
                               code6, len(stocks))
                return stocks
            return []
        self._index_pool_cache[code6] = (now, list(stocks))
        try:
            os.makedirs(os.path.dirname(pool_file), exist_ok=True)
            with open(pool_file, "w", encoding="utf-8") as f:
                _json.dump({"date": _dt.date.today().isoformat(),
                            "stocks": stocks}, f, ensure_ascii=False)
        except Exception:
            pass
        return stocks

    _BAOSTOCK_INDEX = {"000300": "query_hs300_stocks",
                       "000016": "query_sz50_stocks",
                       "000905": "query_zz500_stocks"}

    def _fetch_index_stocks_live(self, code6: str) -> list[str]:
        # 1) baostock 覆盖的中证指数（fields: updateDate, sh.600000, 名称）
        fn = self._BAOSTOCK_INDEX.get(code6)
        if fn:
            try:
                import baostock as _bs
                lg = _bs.login()
                try:
                    if lg and getattr(lg, "error_code", "1") != "0":
                        raise RuntimeError("baostock login failed")
                    rs = getattr(_bs, fn)()
                    out = []
                    while rs.next():
                        row = rs.get_row_data()
                        # fields: [updateDate, 'sh.600000', 名称]
                        if len(row) >= 2 and "." in row[1]:
                            mkt, _, sym = row[1].partition(".")
                            out.append(sym + (".XSHG" if mkt == "sh" else ".XSHE"))
                    if out:
                        return sorted(set(out))
                finally:
                    try:
                        _bs.logout()
                    except Exception:
                        pass
            except Exception:
                logger.warning("index_stocks %s baostock 拉取失败", code6,
                               exc_info=True)
        # 2) 其余走国证官网（399101 中小综指等）
        if code6.startswith(("39", "98")):
            try:
                import requests as _rq
                r = _rq.get(
                    "http://www.cnindex.com.cn/sample-detail/detail",
                    params={"indexcode": code6, "pageNum": 1, "rows": 3000},
                    timeout=20)
                rows = (r.json().get("data") or {}).get("rows") or []
                out = []
                for it in rows:
                    sec = str(it.get("seccode") or "")
                    if sec.isdigit() and len(sec) == 6:
                        # 国证接口仅深市指数成分（000/002/300 开头）
                        out.append(sec + ".XSHE")
                if out:
                    return sorted(set(out))
            except Exception:
                logger.warning("index_stocks %s cnindex 拉取失败", code6,
                               exc_info=True)
        return []

    def _build_name_map(self) -> dict[str, str]:
        """构建 {纯6位代码: 名称} 映射：优先读本地缓存命中，否则本地 instruments（股票）
        + ETF（本地或免费 API），构建后写回缓存。

        名称属展示层：任何失败降级为空/部分映射，不影响行情路径。
        """
        # 0) 缓存命中直接返回（免重复构建/免网络）
        try:
            if os.path.exists(self._names_cache_file):
                with open(self._names_cache_file, encoding="utf-8") as f:
                    cached = _json.load(f)
                if isinstance(cached, dict) and cached:
                    return {str(k): str(v) for k, v in cached.items()}
        except Exception:
            pass
        out: dict[str, str] = {}
        # 1) 股票：本地 instruments parquet（免费档已含全量股票名称）
        try:
            inst = os.path.join(self.data_root, "instruments", "instruments.parquet")
            if os.path.exists(inst):
                df = pl.read_parquet(inst)
                if "symbol" in df.columns and "name" in df.columns:
                    for sym, name in df.select(["symbol", "name"]).iter_rows():
                        if sym and name:
                            out[str(sym).split(".")[0]] = str(name)
        except Exception:
            logger.warning("get_stock_names: instruments 读取失败", exc_info=True)
        # 2) ETF：本地 instruments_etf parquet 优先，缺失则免费 TickFlow API 补
        etf_ok = False
        try:
            import glob as _glob
            etf_paths = _glob.glob(
                os.path.join(self.data_root, "instruments_etf", "**", "*.parquet"),
                recursive=True)
            df_etf = None
            if etf_paths:
                try:
                    df_etf = pl.scan_parquet(etf_paths).collect()
                except Exception:
                    df_etf = None
            if df_etf is None or df_etf.is_empty() or "name" not in df_etf.columns:
                from app.services.index_sync import _fetch_instruments_by_type
                df_etf = _fetch_instruments_by_type("etf", "etf")
            if df_etf is not None and not df_etf.is_empty() \
                    and "symbol" in df_etf.columns and "name" in df_etf.columns:
                for sym, name in df_etf.select(["symbol", "name"]).iter_rows():
                    if sym and name:
                        out.setdefault(str(sym).split(".")[0], str(name))
                        etf_ok = True
        except Exception:
            logger.warning("get_stock_names: ETF 名称获取失败，降级本地", exc_info=True)
        # 3) 落盘缓存：仅当 ETF 段成功（etf_ok）时写，避免 ETF API 失败时
        #    钉住股票-only 映射
        if etf_ok:
            try:
                os.makedirs(os.path.dirname(self._names_cache_file), exist_ok=True)
                with open(self._names_cache_file, "w", encoding="utf-8") as f:
                    _json.dump(out, f, ensure_ascii=False)
            except Exception:
                pass
        return out

    def get_stock_names(self, codes: list[str] | None = None) -> dict[str, str]:
        """返回 {纯6位代码: 名称} 映射；codes 非空时只返回命中的子集。

        恢复 jqengine get_all_securities/get_security_name 的名称解析，
        同时为模拟盘落库提供名称。进程内缓存，首次构建后复用。
        """
        if self._names_map is None:
            self._names_map = self._build_name_map()
        if not codes:
            return dict(self._names_map)
        return {c: n for c, n in self._names_map.items() if c in set(codes)}

    def get_adj_factors(self) -> pl.DataFrame:
        # 因子表仅除权事件/15:35 同步后变化：TTL 300s 去重即可，
        # 避免每次调用 recursive glob + 全量 scan_parquet（含 lf.columns schema 解析）。
        return self.get_or_fetch("adj_factors", 300.0, self._load_adj_factors)

    def get_financials(self) -> pl.DataFrame:
        """全量季频财务长表（tdx gpcw 落盘分区），TTL 600s 去重。"""
        return self.get_or_fetch("financials", 600.0, self._load_financials)

    def _load_financials(self) -> pl.DataFrame:
        from ..tdx_financials import load_financials
        return load_financials()

    def _load_adj_factors(self) -> pl.DataFrame:
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
