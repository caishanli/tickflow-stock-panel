"""数据源聚合：本地分区 / mootdx / astock + 当日分钟内存库 + 共享网络拉取线程池。

内存策略（重要）：
- 本地分区已有的历史数据 → 每次按需读盘，**不常驻内存**（短 TTL 仅突发去重）；
- 本地没有、需网络拿的数据（当日实时分钟）→ 拿到后进**当日分钟内存库**，
  当日驻留，次日 00:00 清空，避免重复回源。
- 当日分钟内存库是纯 lazy dict：服务启动不预载、不预分配，未请求标的零内存。
"""
from __future__ import annotations

import contextlib
import datetime as _dt
import functools
import json as _json
import logging
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import ClassVar, TypeVar
from zoneinfo import ZoneInfo

import polars as pl

from . import rt_sources as _rt
from .single_flight import DedupCache, SingleFlight

logger = logging.getLogger("app.services.stockdata.sources")

_HIST_TTL = 60.0  # 历史日线/分钟短 TTL（仅突发去重，不驻留）

_T = TypeVar("_T")


def _now() -> _dt.datetime:
    """服务端行情时钟统一为北京时间，与分钟分区的无时区时间戳一致。"""
    return _dt.datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)


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
    now = now or _now()
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
            if self._day is None or self._day < day:
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
        target_day = _dt.date.fromisoformat(day[:10])
        frames = [_as_datetime(df) for df in frames if not df.is_empty()]
        with self._lock:
            # 跨日回源可能晚到，不能把新一天的内存库倒退或混入上一日数据。
            if self._day is not None and target_day < self._day:
                return
            if self._day != target_day:
                self._frames.clear()
                self._day = target_day
            for df in frames:
                df = df.filter(pl.col("datetime").dt.date() == target_day)
                syms = set(df["symbol"].to_list())
                for sym in syms:
                    sub = df.filter(pl.col("symbol") == sym)
                    old = self._frames.get(sym)
                    merged = pl.concat([old, sub], how="vertical_relaxed").unique(
                        subset=["datetime"], keep="last").sort("datetime") if old is not None \
                        else sub
                    self._frames[sym] = merged

    def get_slice(self, symbols: set[str], lo_ts: str, hi_ts: str) -> pl.DataFrame:
        """取内存库中指定标的在 [lo_ts, hi_ts] 的当日分钟（空帧当无数据）。"""
        with self._lock:
            parts = [self._frames[s] for s in symbols if s in self._frames]
        if not parts:
            return pl.DataFrame(schema={c: pl.Utf8 for c in _MINUTE_COLS})
        # 帧以新对象替换；锁外过滤快照，避免历史大窗口阻塞实时写入。
        return pl.concat(parts, how="vertical_relaxed").filter(
            (pl.col("datetime") >= pd_to_ts(lo_ts)) & (pl.col("datetime") <= pd_to_ts(hi_ts)))


class DayFileCache:
    """日线日期文件缓存：键=(subdir, date) → 该日全市场整帧（原始单位）。

    日线分区按日存储、每日期文件含全市场标的：读取时整文件载入内存，同文件
    其他标的的后续请求直接命中。后台清扫线程每 10s 卸载超时（默认 60s）未
    访问的文件，并执行容量上限（默认 60 文件）淘汰。不预载、不驻留 400 天
    全市场整帧（spec 2026-08-21-stockdata-daily-dayfile-lru-design）。
    fingerprint 提供目标日文件版本；访问时校验，落盘修复后不继续返回热旧帧。
    """

    def __init__(self, ttl: float = 60.0, cap: int = 60,
                 fingerprint: Callable[[str, str], object] | None = None) -> None:
        self._ttl = ttl
        self._cap = cap
        self._fingerprint = fingerprint
        self._items: dict[tuple[str, str], tuple[float, object, pl.DataFrame]] = {}
        self._lock = threading.Lock()
        self._single = SingleFlight()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def get(self, subdir: str, date: str) -> pl.DataFrame | None:
        """命中返回帧并刷新该文件最后访问时间；未命中返回 None（不加载）。"""
        version = self._fingerprint(subdir, date) if self._fingerprint else None
        with self._lock:
            item = self._items.get((subdir, date))
            if item is None:
                return None
            _ts, loaded_version, frame = item
            if version != loaded_version:
                del self._items[(subdir, date)]
                return None
            self._items[(subdir, date)] = (time.monotonic(), version, frame)
            return frame

    def get_or_load(self, subdir: str, date: str,
                    loader: Callable[[], pl.DataFrame | None]) -> pl.DataFrame | None:
        """缓存命中直接返回；未命中日期文件级 single-flight 读盘（同键并发只读一次）。"""
        hit = self.get(subdir, date)
        if hit is not None:
            return hit
        def load_if_missing():
            cached = self.get(subdir, date)
            if cached is not None:
                return cached
            # 必须在读帧之前记录版本；读盘期间发生替换，下次访问仍能检测到。
            version = self._fingerprint(subdir, date) if self._fingerprint else None
            frame = loader()
            if frame is None or frame.is_empty():
                return None
            self._insert(subdir, date, version, frame)
            return frame

        return self._single.run(f"{subdir}:{date}", load_if_missing)

    def _insert(self, subdir: str, date: str, version: object, frame: pl.DataFrame) -> None:
        with self._lock:
            self._items[(subdir, date)] = (time.monotonic(), version, frame)

    def sweep(self) -> int:
        """卸载超时未访问文件；仍超容量上限时按最后访问时间从旧到新踢。返回卸载数。"""
        now = time.monotonic()
        evicted = 0
        with self._lock:
            for k in [k for k, (ts, _v, _f) in self._items.items() if now - ts > self._ttl]:
                del self._items[k]
                evicted += 1
            if len(self._items) > self._cap:
                oldest = sorted(self._items.items(), key=lambda kv: kv[1][0])
                for k, _v in oldest[: len(self._items) - self._cap]:
                    del self._items[k]
                    evicted += 1
        return evicted


class NetworkPuller:
    """共享实时拉取编排：结果TTL缓存 → mootdx冷启动自举 → 腾讯批量 → 新浪批量
    → mootdx逐只兜底。batch 级加锁串行化，避免并发客户端重复批量请求；
    forced=mootdx 时完全走旧逐只路径（一键回滚）。"""

    def __init__(self, factory: Callable | None = None, workers: int = 16):
        self._factory = factory
        self._workers = max(1, workers)
        self._local = threading.local()
        self._pool = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="stockdata-pull")
        self._chain_lock = threading.Lock()
        try:
            self._result_ttl = float(os.getenv("STOCKDATA_RT_RESULT_TTL", "") or 3.0)
        except (TypeError, ValueError):
            self._result_ttl = 3.0
        self._result_cache: dict[str, tuple[float, pl.DataFrame]] = {}
        self._bootstrapped: set[str] = set()
        self._bootstrap_day: _dt.date | None = None
        self.synth = _rt.BarSynthesizer()
        forced = os.getenv("STOCKDATA_RT_SOURCE", "auto") or "auto"
        self.tencent = None if forced in ("mootdx", "sina") else _rt.TencentRTSource()
        self.sina = None if forced in ("mootdx", "tencent") else _rt.SinaRTSource()

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

    def _cache_put(self, tf: str, frame: pl.DataFrame) -> None:
        self._result_cache[tf] = (time.monotonic(), frame)

    def _bootstrap_needed(self, code_tf: str) -> bool:
        """交易时段冷启动自举判定：当日既无合成记录也无结果缓存。"""
        if not _in_trading():
            return False
        has_record = (code_tf in self._bootstrapped
                      or code_tf in self._result_cache
                      or self.synth.last_quote_time(code_tf) is not None)
        return not has_record

    def _pull_mootdx_one(self, code_tf: str) -> pl.DataFrame | None:
        """mootdx 单只实时分钟拉取（超时重置线程源）。返回帧或 None。"""
        try:
            df = _pull_recent_guarded(self._source(), code_tf)
        except TimeoutError:
            # 超时：复用 socket 可能已坏，重置本线程数据源，下次拉取重建
            self._local.src = None
            return None
        if df is None or df.empty:
            return None
        pdf = df.reset_index()
        pdf["symbol"] = code_tf
        for c in _MINUTE_COLS:
            if c not in pdf.columns:
                pdf[c] = None
        frame = _as_datetime(pl.from_pandas(pdf[_MINUTE_COLS]))
        frame = frame.filter(pl.col("datetime").dt.date() == _now().date())
        return frame if not frame.is_empty() else None

    def _seed_synth_from_frame(self, tf: str, frame: pl.DataFrame) -> None:
        """自举帧末行播种合成器：半程真实 bar + 累计量额基线（防首拍零量覆盖）。

        mootdx 最近一页可能跨多个交易日；累计基线只累加今日量额。
        """
        try:
            frame = _as_datetime(frame).filter(
                (pl.col("symbol") == tf) & (pl.col("datetime").dt.date() == _now().date()))
            rows = frame.sort("datetime").tail(1).to_dicts()
            if not rows:
                return
            last = rows[0]
            self.synth.seed(tf, last["datetime"],
                            float(last["open"]), float(last["high"]),
                            float(last["low"]), float(last["close"]),
                            float(last["volume"]), float(last["amount"]),
                            float(frame["volume"].sum() or 0.0),
                            float(frame["amount"].sum() or 0.0))
        except Exception as e:  # noqa: BLE001
            logger.warning("[sources] %s 合成器播种失败: %s", tf, e)

    def fetch_many(self, codes: list[str]) -> list[pl.DataFrame]:
        with self._chain_lock:
            today = _now().date()
            if self._bootstrap_day != today:
                self._bootstrap_day = today
                self._bootstrapped.clear()
                # 在 TTL 查询之前换日，避免昨日结果命中而绕过自举/合成器重置。
                self._result_cache.clear()
                self.synth.reset_if_new_day(today)
            tf_codes = [_tf_symbol(c) for c in codes]
            out: dict[str, pl.DataFrame] = {}
            todo: list[str] = []
            now_mono = time.monotonic()
            # ① 结果 TTL 缓存：命中直接复用帧不再触网
            for tf in dict.fromkeys(tf_codes):
                hit = self._result_cache.get(tf)
                if hit is not None and now_mono - hit[0] < self._result_ttl:
                    out[tf] = hit[1]
                else:
                    todo.append(tf)
            # ② 冷启动自举：交易时段、无任何当日记录的标的，mootdx 拉「今日迄今」一次。
            # 池内并行（与⑤同款线程本地源）——自举发生在链锁内，串行会把冷启动后
            # 首个请求拖成 N×单只耗时并堵死所有并发 fetch_many
            boot_syms = [tf for tf in todo if self._bootstrap_needed(tf)]
            if boot_syms:
                futures = {self._pool.submit(self._pull_mootdx_one, c): c
                           for c in boot_syms}
                for f, tf in futures.items():
                    try:
                        frame = f.result()
                    except Exception as e:  # noqa: BLE001
                        logger.warning("[sources] %s 自举失败(非超时): %s", tf, e)
                        frame = None
                    self._bootstrapped.add(tf)
                    if frame is not None and not frame.is_empty():
                        out[tf] = frame
                        self._cache_put(tf, frame)
                        todo.remove(tf)
                        self._seed_synth_from_frame(tf, frame)
            # ③④ 腾讯批量 → 新浪批量 → 合成；forced=mootdx 跳过 HTTP 链
            forced = os.getenv("STOCKDATA_RT_SOURCE", "auto") or "auto"
            if forced != "mootdx" and todo:
                quotes: dict[str, _rt.RTQuote] = {}
                if self.tencent is not None:
                    quotes.update(self.tencent.fetch(todo))
                missing = [s for s in todo if s not in quotes]
                if missing and self.sina is not None:
                    quotes.update(self.sina.fetch(missing))
                emitted: set[str] = set()
                if quotes:
                    self.synth.reset_if_new_day(today)
                    for df in self.synth.update(quotes):
                        for sym in df["symbol"].unique().to_list():
                            sub = df.filter(pl.col("symbol") == sym)
                            out[sym] = sub
                            self._cache_put(sym, sub)
                            emitted.add(sym)
                # 快照在册但未出 bar（停牌冻结时刻被合成器守卫跳过等）→ 与解析
                # 缺失同等下传 mootdx 兜底
                todo = [s for s in todo if s not in emitted]
            # ⑤ mootdx 逐只兜底（池内并行）
            if todo:
                futures = {self._pool.submit(self._pull_mootdx_one, c): c
                           for c in todo}
                for f, tf in futures.items():
                    try:
                        frame = f.result()
                    except Exception as e:  # noqa: BLE001
                        logger.warning("[sources] mootdx 兜底失败 %s: %s", tf, e)
                        continue
                    if frame is not None and not frame.is_empty():
                        out[frame["symbol"][0]] = frame
                        self._cache_put(frame["symbol"][0], frame)
            return list(out.values())

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False)
        if self.tencent is not None:
            self.tencent.close()
        if self.sina is not None:
            self.sina.close()


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
        err = box["err"]
        from app.quant.jqengine.datasource.mootdx_breaker import BREAKER_OPEN_MSG
        if BREAKER_OPEN_MSG in str(err):
            return None  # 熔断开路：快速失败已由熔断器统一日志，逐只不再刷屏
        logger.warning("[sources] %s 实时回源失败: %s", code, err)
        return None
    df = box.get("df")
    if df is None or df.empty:
        return None
    return df


class DataSources:
    """聚合源：本地分区读取为主 + 当日分钟内存库 + 共享网络拉取池。"""

    def __init__(self, data_root: str | None = None, mootdx_factory: Callable | None = None,
                 fetch_workers: int | None = None):
        self.data_root = (data_root
                          or os.getenv("PARTITION_DATA_ROOT")
                          or os.path.join(os.path.dirname(__file__),
                                          "..", "..", "..", "..", "data"))
        if fetch_workers is None:
            try:
                fetch_workers = int(os.getenv("STOCKDATA_FETCH_WORKERS", "") or 16)
            except (TypeError, ValueError):
                fetch_workers = 16
        self.dedup = DedupCache()
        self.minute_store = MinuteMemoryStore()
        self.dayfile_cache = DayFileCache(fingerprint=self._day_file_version)
        self.puller = NetworkPuller(factory=mootdx_factory, workers=fetch_workers)
        self._names_map: dict[str, str] | None = None
        self._names_cache_file = os.path.join(self.data_root, ".stock_names_cache.json")
        self._index_pool_cache: dict[str, tuple[float, list[str]]] = {}

    # ---- 去重透传 ----
    def get_or_fetch(self, key: str, ttl: float, loader: Callable[[], _T]) -> _T:
        return self.dedup.get_or_fetch(key, ttl, loader)

    # ---- 分区扫描 ----
    def _day_file_version(self, subdir: str, date: str) -> tuple:
        """只 stat 目标日文件；回源原子替换/增删分片后热缓存立即失效。"""
        root = os.path.join(self.data_root, subdir, f"date={date}")
        try:
            with os.scandir(root) as entries:
                versions = []
                for entry in entries:
                    if entry.name.endswith(".parquet"):
                        try:
                            st = entry.stat()
                        except FileNotFoundError:
                            continue
                        versions.append((entry.name, st.st_ino, st.st_size,
                                         st.st_mtime_ns, st.st_ctime_ns))
                return tuple(sorted(versions))
        except FileNotFoundError:
            return ()

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
                    subdir, day, functools.partial(self._read_day_file, subdir, day))
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
        lo = str(pd_to_date(start_date)) if start_date else None
        hi = str(pd_to_date(end_date)) if end_date else None

        syms = {_tf_symbol(c) for c in codes}
        parts = []
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False),
                                 ("kline_index_daily", False)):
            for day in self._existing_day_files(subdir, lo, hi):
                frame = self.dayfile_cache.get_or_load(
                    subdir, day, functools.partial(self._read_day_file, subdir, day))
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

    def apply_qfq_daily(self, df: pl.DataFrame) -> pl.DataFrame:
        """对日线帧应用**最新锚定前复权**（price × ex_factor(date)）。

        因子表 adj_factor_etf 逐日行即"锚定最新的累计因子"（最新日=1.0），
        直接按 (symbol, date) 左连乘到 OHLC；无因子记录的标的原样（×1.0）。
        最新锚定与请求区间无关 → 服务端对同一标的数据口径恒定，客户端
        （模拟盘 manager 日线缓存）跨请求无陈旧因子问题。
        仅日线调用（分钟/当前价保持真实成交价）。
        注意 symbol 口径：日线分区为 tushare 码（561980.SH），因子表为
        聚宽码（561980.XSHG），join 前归一到 tushare 码。
        """
        if df.is_empty() or "symbol" not in df.columns or "date" not in df.columns:
            return df
        fac = self.get_adj_factors()
        if fac.is_empty():
            return df
        fdf = fac.select(
            pl.col("symbol").str.replace(".XSHG", ".SH", literal=True)
            .str.replace(".XSHE", ".SZ", literal=True).alias("symbol"),
            pl.col("trade_date").cast(pl.Date).alias("date"),
            pl.col("ex_factor"),
        ).unique(subset=["symbol", "date"], keep="last").sort(
            ["symbol", "date"])
        out = df.with_columns(pl.col("date").cast(pl.Date).alias("date")).join(
            fdf, on=["symbol", "date"], how="left").with_columns(
            pl.col("ex_factor").fill_null(1.0))
        for col in ("open", "high", "low", "close"):
            if col in out.columns and out.schema[col] in (pl.Float64, pl.Float32,
                                                          pl.Int64, pl.Int32):
                out = out.with_columns(
                    (pl.col(col).cast(pl.Float64) * pl.col("ex_factor")).alias(col))
        return out.drop("ex_factor")

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
        """历史分区短 TTL 去重；实时内存每次叠加，刷新后立即对回测/模拟盘可见。"""
        if not codes:
            return pl.DataFrame()
        lo_d = str(pd_to_date(lo_ts)) if lo_ts is not None else None
        hi_d = str(pd_to_date(hi_ts)) if hi_ts is not None else None
        syms = {_tf_symbol(c) for c in codes}

        def _load():
            parts = []
            for subdir in ("kline_etf_minute", "kline_minute"):
                df = self._scan_partitions(subdir, lo_d, hi_d, syms, _MINUTE_COLS)
                if not df.is_empty():
                    parts.append(df)
            if not parts:
                return pl.DataFrame()
            out = pl.concat(parts, how="vertical_relaxed")
            if lo_ts is not None:
                out = out.filter(pl.col("datetime") >= pd_to_ts(lo_ts))
            if hi_ts is not None:
                out = out.filter(pl.col("datetime") <= pd_to_ts(hi_ts))
            return out.unique(subset=["symbol", "datetime"], keep="last")

        key = f"min:{','.join(sorted(syms))}:{lo_ts}:{hi_ts}"
        persisted = self.get_or_fetch(key, 10.0, _load)
        today = _now().date()
        if (lo_d is None or lo_d <= today.isoformat()) and (hi_d is None or hi_d >= today.isoformat()):
            mem = self.minute_store.get_slice(
                syms, str(lo_ts or today), str(hi_ts or f"{today} 15:00:00"))
            if not mem.is_empty():
                parts = [persisted, mem] if not persisted.is_empty() else [mem]
                return pl.concat(parts, how="vertical_relaxed").unique(
                    subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
        # 纯历史缓存命中不重复拼帧/去重，保持回测大窗口的常数时间返回路径。
        return persisted

    def get_realtime_snapshot(self, codes: list[str], as_of=None) -> pl.DataFrame:
        """按 as-of 返回股票/ETF 当日分钟；只有真实今日的交易时段允许实时回源。

        历史补跑只读分区，不改变实时库日期。回源帧限定目标日，未来 bar 在
        新鲜度判定之前过滤，避免收盘分区的未来行掩盖当前分钟缺口。
        """
        now = _now()
        asof_ts = pd_to_ts(as_of) if as_of is not None else now
        day = asof_ts.date()
        day_start = _dt.datetime.combine(day, _dt.time())
        tf_syms = {_tf_symbol(c) for c in codes}
        empty = pl.DataFrame(schema={c: pl.Utf8 for c in _MINUTE_COLS})
        if not tf_syms:
            return empty
        is_today = day == now.date()
        if is_today:
            self.minute_store.ensure_day(day)

        def window(frame):
            return _as_datetime(frame).filter(
                pl.col("symbol").is_in(tf_syms)
                & (pl.col("datetime") >= day_start)
                & (pl.col("datetime") <= asof_ts))

        base_parts = []
        for subdir in ("kline_etf_minute", "kline_minute"):
            part = self.dayfile_cache.get_or_load(
                subdir, day.isoformat(),
                functools.partial(self._read_day_file, subdir, day.isoformat(), _MINUTE_COLS))
            if part is not None and not part.is_empty():
                sub = window(part)
                if not sub.is_empty():
                    base_parts.append(sub)
        if is_today:
            mem = self.minute_store.get_slice(tf_syms, str(day_start), str(asof_ts))
            if not mem.is_empty():
                base_parts.append(mem)
        base = pl.concat(base_parts, how="vertical_relaxed").unique(
            subset=["symbol", "datetime"], keep="last") if base_parts else empty

        try:
            stale_sec = float(os.getenv("STOCKDATA_RT_STALE_SEC", "") or 10.0)
        except ValueError:
            stale_sec = 10.0

        def is_stale(sym, last_dt):
            qt = self.puller.synth.last_quote_time(sym)
            # 另一个客户端可能已请求更晚时点，未来快照不能豁免本次 as-of 缺口。
            eff = max(last_dt, qt) if qt is not None and qt <= asof_ts else last_dt
            return eff < asof_ts - _dt.timedelta(seconds=stale_sec)

        latest_by_sym = dict(base.group_by("symbol").agg(pl.col("datetime").max()).iter_rows())
        todo = [c for c in dict.fromkeys(codes)
                if is_today and _in_trading(now) and _in_trading(asof_ts) and not _is_index(c)
                and (_tf_symbol(c) not in latest_by_sym
                     or is_stale(_tf_symbol(c), latest_by_sym[_tf_symbol(c)]))]
        if todo:
            for frame in self.puller.fetch_many(todo):
                if frame.is_empty():
                    continue
                frame = window(frame)
                if frame.is_empty():
                    continue
                self.minute_store.update(day.isoformat(), frame)
                base_parts.append(frame)
        if not base_parts:
            return empty
        return pl.concat(base_parts, how="vertical_relaxed").unique(
            subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])

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

    _BAOSTOCK_INDEX: ClassVar[dict] = {"000300": "query_hs300_stocks",
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
                    with contextlib.suppress(Exception):
                        _bs.logout()
            except Exception:
                logger.warning("index_stocks %s baostock 拉取失败", code6,
                               exc_info=True)
        # 2) 其余走国证官网（399101 中小综指等）
        if code6.startswith(("39", "98")):
            try:
                import requests as _rq
                params: dict[str, str | int] = {
                    "indexcode": code6, "pageNum": 1, "rows": 3000}
                r = _rq.get(
                    "http://www.cnindex.com.cn/sample-detail/detail",
                    params=params,
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
        """因子表 = ETF（adj_factor_etf，mootdx xdxr 重建）+ 股票（adj_factor，
        mootdx 扩展段 / TickFlow ex_factors 同目录），两者 schema 相同直接拼接。

        股票目录缺失/为空不报错（未跑过股票回源的部署因子只有 ETF 段）；
        排除 xdxr 事件表（同目录不同 schema，混入 scan 会炸掉整次加载）。"""
        import glob as _glob
        frames = []
        for subdir in ("adj_factor_etf", "adj_factor"):
            root = os.path.join(self.data_root, subdir)
            if not os.path.isdir(root):
                continue
            paths = _glob.glob(os.path.join(root, "**", "*.parquet"),
                               recursive=True)
            paths = [p for p in paths
                     if not os.path.basename(p).startswith("xdxr_events")]
            if not paths:
                continue
            lf = pl.scan_parquet(paths, hive_partitioning=True)
            cols = lf.columns
            if "symbol" not in cols:
                lf = lf.with_columns(pl.lit("").alias("symbol"))
            frames.append(lf.select(["symbol", "trade_date", "ex_factor"]).collect())
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="vertical_relaxed")


def pd_to_ts(x):
    import pandas as pd
    return pd.Timestamp(x)


def pd_to_date(x):
    import pandas as pd
    return pd.Timestamp(x).date()
