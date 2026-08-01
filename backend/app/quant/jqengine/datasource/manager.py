"""数据源优先级调度 + 自动降级 + 缓存管理器。"""

import logging
import os
from collections import OrderedDict
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import set_key

logger = logging.getLogger("jqengine.dm")

from .cache import DataCache
from .mootdx_src import MootdxSource
from .astock_src import AStockSource
from .base import DataSourceError
from ..config import CONFIG, REPO_ROOT


# ---------------------------------------------------------------------------
# DuckDB 数据源：直接读 stock.duckdb，不依赖外部 parquet 缓存
# ---------------------------------------------------------------------------
_DUCKDB_PATH = os.getenv(
    "TICKFLOW_DB_PATH",
    str(Path(REPO_ROOT).parent / "data" / "stock.duckdb"),
)


def _jq_to_duckdb(code: str) -> str:
    """聚宽代码 -> DuckDB symbol（两者均用 .XSHG/.XSHE 格式，直接透传）。"""
    return code


class DuckDBSource:
    """直接从 stock.duckdb 读取日线/分钟线，免 parquet 缓存。"""

    name = "duckdb"

    def __init__(self):
        self._conn = None
        self._etf_symbols: set[str] | None = None

    def _get_etf_symbols(self) -> set[str]:
        if self._etf_symbols is None:
            conn = self._get_conn()
            try:
                rows = conn.execute("SELECT symbol FROM instruments_etf").fetchall()
                self._etf_symbols = {r[0] for r in rows}
            except Exception:
                self._etf_symbols = set()
        return self._etf_symbols

    def _get_conn(self):
        if self._conn is None:
            import duckdb
            self._conn = duckdb.connect(_DUCKDB_PATH, read_only=True)
        return self._conn

    def get_daily(self, code, start, end):
        sym = _jq_to_duckdb(code)
        conn = self._get_conn()
        etf_syms = self._get_etf_symbols()
        table = "kline_etf_daily" if sym in etf_syms else "kline_daily"
        try:
            rows = conn.execute(
                f"SELECT date, open, high, low, close, volume, amount "
                f"FROM {table} WHERE symbol = ? AND date >= ? AND date <= ? "
                f"ORDER BY date",
                [sym, str(start)[:10], str(end)[:10]],
            ).fetchall()
            if not rows:
                raise DataSourceError(f"duckdb 无日线: {code}")
            df = pd.DataFrame(
                rows, columns=["date", "open", "high", "low", "close", "volume", "amount"]
            )
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            return df
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"duckdb 日线失败: {e}")

    def get_minute(self, code, date, start_date=None):
        sym = _jq_to_duckdb(code)
        conn = self._get_conn()
        etf_syms = self._get_etf_symbols()
        table = "kline_etf_minute" if sym in etf_syms else "kline_minute"
        try:
            rows = conn.execute(
                f"SELECT datetime, open, high, low, close, volume, amount "
                f"FROM {table} WHERE symbol = ? AND datetime::DATE = ? "
                f"ORDER BY datetime",
                [sym, str(date)[:10]],
            ).fetchall()
            if not rows:
                raise DataSourceError(f"duckdb 无分钟: {code} {date}")
            df = pd.DataFrame(
                rows, columns=["datetime", "open", "high", "low", "close", "volume", "amount"]
            )
            df["datetime"] = pd.to_datetime(df["datetime"])
            return df
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"duckdb 分钟失败: {e}")

    def get_etf_list(self):
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT symbol FROM instruments_etf").fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    def get_stock_list(self):
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT symbol FROM instruments").fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    def get_index_realtime(self, codes):
        raise DataSourceError("duckdb 源不提供实时指数")

    def get_us_index(self):
        raise DataSourceError("duckdb 源不提供美股")

    def test_connection(self):
        try:
            self._get_conn().execute("SELECT 1")
            return True
        except Exception:
            return False


SOURCES = {"duckdb": DuckDBSource, "mootdx": MootdxSource, "astock": AStockSource}

# --- DataManager 单例：确保策略与 JqDataSource 共享同一缓存实例 ---
_data_manager_instance = None


def get_data_manager(token=None, cache=None):
    """获取 DataManager 单例实例。"""
    global _data_manager_instance
    if _data_manager_instance is None:
        _data_manager_instance = DataManager(token=token, cache=cache)
    return _data_manager_instance


class _MinuteLRU(OrderedDict):
    """有界 LRU 分钟帧缓存（code -> DataFrame）。

    M11 修复：原 ``_minute_mem`` 为无界 dict，分钟帧被永久持有，外部 LRU
    驱逐不释放内存（数百只 ETF × 全窗口 1m 帧可达 GB 级）。现命中
    （``__getitem__``/``get``）即移到队首；写入超出容量时从队尾逐出最久
    未用项，帧引用随之释放，并通过 ``on_evict`` 回调通知（DataManager
    借此同步清理 ``_minute_cov`` 覆盖区间元数据）。

    对外读取接口与普通 dict 一致（``get`` / ``in`` / ``len`` / 真值判断），
    api.py 等直接读 ``mgr._minute_mem`` 的调用方无需改动。
    """

    def __init__(self, cap=800, on_evict=None):
        super().__init__()
        self.cap = max(1, int(cap))
        self._on_evict = on_evict

    def __getitem__(self, key):
        val = super().__getitem__(key)
        self.move_to_end(key)
        return val

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        while len(self) > self.cap:
            k, v = self.popitem(last=False)
            if self._on_evict is not None:
                self._on_evict(k, v)


def _ensure_money_yuan(df, src_name):
    """保证日线帧带 ``money`` 列（单位：元）。

    各数据源 amount 单位不一：mootdx/astock 为元；mootdx 源已自带
    money(元)（见 mootdx_src.get_daily 的 amount→money 映射）。未归一时直接
    把 amount 当元用会让全市场成交额聚合失真，流动性门槛/池过滤全面失真。
    """
    if df is None or getattr(df, "empty", True):
        return df
    if "money" in df.columns:
        return df
    if "amount" not in df.columns:
        return df
    df = df.copy()
    df["money"] = pd.to_numeric(df["amount"], errors="coerce")
    return df


def _ensure_volume_shares(df, src_name):
    """保证日线帧带 ``volume`` 列（单位：股）。

    mootdx 源内已 vol×100→volume(股)（见 mootdx_src.get_daily），astock
    的 volume 单位即为股（astock 实测小 ETF 约 5e7 量级），已有 ``volume``
    列的帧不动。
    """
    if df is None or getattr(df, "empty", True):
        return df
    if "volume" in df.columns:
        return df
    if "vol" not in df.columns:
        return df
    df = df.copy()
    df["volume"] = pd.to_numeric(df["vol"], errors="coerce")
    return df


def _infer_adj(df, src_name):
    """推断日线帧的复权口径：优先读 ``df.attrs["adj"]``（各源 get_daily
    已标注）；旧缓存帧（pickle/parquet）没有 attrs，按源名回退推断——
    mootdx 通达信原始价按 "raw"、其余（如 astock）按 "unknown"。
    """
    adj = getattr(df, "attrs", None)
    adj = adj.get("adj") if adj else None
    if adj:
        return adj
    return {"mootdx": "raw"}.get(src_name, "unknown")


def _pick_daily_frame(candidates):
    """同一代码多源缓存并存时的选帧：按优先级取第一个非 raw 帧，全 raw 才用 raw。

    背景：mootdx 为通达信原始不复权价（raw）；若只按源优先级选帧，
    同一回测内价格帧可能来自不同复权口径。``candidates`` 为
    ``(pri, src_name, key, df)`` 元组、已按优先级升序；"非 raw" 含
    qfq/unknown（unknown 可能是前复权，raw 一定不是）。返回选中的元组；
    无候选返回 None。
    """
    first_raw = None
    for cand in candidates:
        if _infer_adj(cand[3], cand[1]) == "raw":
            if first_raw is None:
                first_raw = cand
            continue
        return cand
    return first_raw


class DataManager:
    """按 ``DATASOURCE_PRIORITY`` 顺序尝试各数据源，单源失败自动降级到下一级。

    ``get_daily`` / ``get_minute`` 走本地缓存（按 ``{源}_{代码}`` 分键），
    重复读取不重复请求接口。
    """

    def __init__(self, token=None, cache=None, minute_mem_cap=None):
        self.cache = cache or DataCache()
        self.sources = {k: v() for k, v in SOURCES.items()}
        # 历史分钟线：优先走真实源（mootdx 分页回看）
        self._minute_win = None  # (start, end) 回测窗口
        self._minute_cov = {}  # code -> (lo_ts, hi_ts) 缓存帧已覆盖区间
        # M11 修复：分钟帧内存缓存改为有界 LRU（原无界 dict 永久持有帧，
        # 数百只 ETF × 全窗口 1m 帧可达 GB 级）。容量取 minute_mem_cap 参数
        # 或 MINUTE_MEM_CAP 环境变量，默认 800（与 scripts/run_jq_rqalpha.py
        # --minute_cache_cap 默认值一致）；驱逐时连带清掉覆盖区间元数据。
        if minute_mem_cap is None:
            try:
                minute_mem_cap = int(os.getenv("MINUTE_MEM_CAP", "") or 800)
            except (TypeError, ValueError):
                minute_mem_cap = 800
        self._minute_mem = _MinuteLRU(
            cap=minute_mem_cap,
            on_evict=lambda k, _v: self._minute_cov.pop(k, None))
        self._minute_empty = set()  # 已知无分钟数据的标的，避免重复网络请求
        self._daily_mem = {}
        # _daily_mem 数据版本号：每次写入/删除递增，money memo 键含版本据此失效
        self._daily_ver = 0
        self._money_memo = {}  # (codes_tuple, daily_ver) -> 全量成交额明细 DataFrame
        # 分钟线滑窗：回测中不持有整个回测区间的分钟数据，只保留最近 N 天，
        # 既省内存又避免每次按需加载都展开整段历史（见 get_minute / _ensure_minute_windowed）
        self.minute_lookback = pd.Timedelta(days=15)
        self._minute_real_cov = {}  # code -> (min_ts, max_ts) mootdx 真实分钟覆盖区间
        self._offline = False       # 回测离线模式：本地优先，缺失不联网回源
        # 数据源取数失败计数：同一源连续返回空/异常 N 次即自动降级到末位并持久化，
        # 避免低效源反复先试。
        self._src_fail = {}
        self._demote_threshold = 3
        self._demoted = set()

    def set_minute_window(self, start, end):
        """设定分钟线展开区间（回测前调用），避免生成超长历史。

        注意：只重置分钟线缓存(_minute_mem)及其覆盖区间元数据(_minute_cov)，
        不清空日线缓存(_daily_mem)。日线数据与分钟窗口无关；若在此清空
        _daily_mem，会使 preload_daily() 预加载的 1628 只ETF日线失效，
        导致回测首日 get_price 批量日线查询返回空、策略退化为防御模式
        (511880)，与聚宽参考产生系统性偏离。
        H6c 修复：旧实现的 _minute_cov 不随缓存重置而清空，新窗口下会拿
        旧窗口的覆盖区间误判命中；现随帧一并清空。
        """
        self._minute_win = (pd.Timestamp(start), pd.Timestamp(end))
        # clear() 而非重新赋值：保持 _minute_mem 对象身份（LRU 实例）不变
        self._minute_mem.clear()
        self._minute_cov.clear()

    @staticmethod
    def _to_jq_code(code):
        """代码(.SZ/.SH) -> JQ代码(.XSHE/.XSHG)。"""
        if "." not in code:
            return code
        pure, suffix = code.split(".", 1)
        su = suffix.upper()
        if su == "SZ":
            return f"{pure}.XSHE"
        if su == "SH":
            return f"{pure}.XSHG"
        return code

    def preload_minute(self, codes):
        """预加载指定标的的分钟线到 _minute_mem（整段窗口，供时钟 feed 使用）。

        一次性把分钟线缓存整库读入内存（``get_all``），再逐标的合并，
        避免回测中反复打开小文件。预加载的标的一般是时钟 feed 与候选池，
        其余标的由策略按需走滑窗加载。
        """
        all_min = self.cache.get_all("minute")
        win = self._minute_win
        count = 0
        for code in codes:
            try:
                df = self._load_minute_merged(code, all_min, full=True)
                if df is not None and not df.empty:
                    self._minute_mem[code] = df
                    if win:
                        # 记录实际覆盖区间（上界归一到当日 15:00），供命中校验
                        self._minute_cov[code] = (win[0], self._hi_eff(win[1]))
                    count += 1
            except Exception:
                continue
        print(f"[preload] 分钟线: {count}/{len(codes)} 只")

    def _preload_from_duckdb(self):
        """从 DuckDB 批量加载全市场日线到 _daily_mem。"""
        from collections import defaultdict
        src = self.sources.get("duckdb")
        if src is None:
            return False
        try:
            conn = src._get_conn()

            # 批量查 kline_daily（stock）
            stock_rows = conn.execute(
                "SELECT symbol, date, open, high, low, close, volume, amount "
                "FROM kline_daily ORDER BY symbol, date"
            ).fetchall()
            # 批量查 kline_etf_daily（ETF）
            etf_rows = conn.execute(
                "SELECT symbol, date, open, high, low, close, volume, amount "
                "FROM kline_etf_daily ORDER BY symbol, date"
            ).fetchall()

            all_rows = stock_rows + etf_rows
            if not all_rows:
                return False

            # 按 symbol 分组
            by_sym = defaultdict(list)
            for row in all_rows:
                by_sym[row[0]].append(row)

            cols = ["date", "open", "high", "low", "close", "volume", "amount"]
            count = 0
            for sym, rows in by_sym.items():
                df = pd.DataFrame(rows, columns=["symbol"] + cols)
                df = df.drop(columns=["symbol"])
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date")
                df = _ensure_money_yuan(df, "duckdb")
                df = _ensure_volume_shares(df, "duckdb")
                # 转 JQ 代码格式
                jq_code = self._to_jq_code(sym)
                self._daily_mem[f"get_daily_{jq_code}"] = df
                count += 1

            self._daily_ver += 1
            print(f"[preload] DuckDB 日线: {count} 只, {len(all_rows)} 行")
            return True
        except Exception as e:
            logger.warning("DuckDB preload 失败: %s", e)
            return False

    def preload_daily(self):
        """一次性加载全部日线缓存到内存，避免回测中逐文件读取。

        优先从 DuckDB 批量加载；DuckDB 不可用时 fallback 到 parquet 缓存。
        """
        # 优先从 DuckDB 批量加载
        if self._preload_from_duckdb():
            return
        # DuckDB 不可用时 fallback 到 parquet 缓存（兼容旧模式）
        all_daily = self.cache.get_all("daily")
        if not all_daily:
            print("[preload] 日线缓存为空")
            return
        priority = self._priority()
        # 收集每只标的的全部源候选帧（按优先级升序），交由 _pick_daily_frame
        # 做"qfq 优先于 raw"的混源防护选帧（空帧不参与候选）
        cands = {}  # jq_code -> [(pri_idx, src_name, key, df)]
        for key, df in all_daily.items():
            if "_" not in key:
                continue
            src_name, raw_code = key.split("_", 1)
            if df is None or (hasattr(df, "empty") and df.empty):
                continue
            jq_code = self._to_jq_code(raw_code)
            pri = priority.index(src_name) if src_name in priority else len(priority)
            cands.setdefault(jq_code, []).append((pri, src_name, key, df))
        count = 0
        for jq_code, lst in cands.items():
            lst.sort(key=lambda c: c[0])
            picked = _pick_daily_frame(lst)
            if picked is None:
                continue
            src_name, key, df = picked[1], picked[2], picked[3]
            # 预计算 trade_dt 列（date 对象），避免 get_daily_money_cached
            # 每天对每只重复 pd.to_datetime（全市场 1600+ 只 × 130 天极慢）。
            if "trade_dt" not in df.columns:
                if "trade_date" in df.columns:
                    df = df.copy()
                    df["trade_dt"] = pd.to_datetime(
                        df["trade_date"].astype(str)).dt.date
                elif "datetime" in df.columns:
                    df = df.copy()
                    df["trade_dt"] = pd.to_datetime(
                        df["datetime"]).dt.date
            # 成交额单位归一（amount → money，单位：元），下游
            # get_daily_money_cached / total_turnover 统一用 money(元)
            df = _ensure_money_yuan(df, src_name)
            # 成交量单位归一（vol → volume，单位：股），与 money 同模式
            df = _ensure_volume_shares(df, src_name)
            # 确保 index 是 DatetimeIndex：批量路径 _get_price_batch_daily
            # 依赖 isinstance(idx, DatetimeIndex) 做日期切片，若为 RangeIndex
            # 会被全部跳过导致 get_price 返回空。
            if not isinstance(df.index, pd.DatetimeIndex):
                if "trade_date" in df.columns:
                    df.index = pd.to_datetime(df["trade_date"].astype(str))
                elif "datetime" in df.columns:
                    df.index = pd.to_datetime(df["datetime"])
                elif "timestamp" in df.columns:
                    df.index = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
            self._daily_mem[f"get_daily_{jq_code}"] = df
            count += 1
        # 日线内存已整体刷新：递增数据版本号，使 money memo 按新版本键重建
        self._daily_ver += 1
        print(f"[preload] 日线: {count} 只ETF, {len(cands)} 缓存键")

    def _priority(self):
        return [s for s in CONFIG["DATASOURCE_PRIORITY"] if s in self.sources]

    def fetch(self, method, *args, **kwargs):
        if method == "get_minute":
            code = args[0]
            end_date = args[1] if len(args) > 1 else None
            start_date = args[2] if len(args) > 2 else None
            return self.get_minute(code, end_date, start_date)
        cache_key = f"{method}_{args[0] if args else ''}"
        # C3：daily 本地优先。_daily_mem 命中即返回；未命中或覆盖不足时，
        # cache.get 会调 loader 回源（mootdx）并落盘。回测中
        # preload_daily 已预加载全量日线，此处一般命中内存。
        if cache_key in self._daily_mem:
            mem = self._daily_mem[cache_key]
            if mem is not None and not (hasattr(mem, "empty") and mem.empty):
                # 内存命中仍需检查是否覆盖请求区间：preload 可能加载了截断的本地
                # 日线（如某 ETF 本地只到 1/30），若不检查覆盖会误当完整返回，
                # 导致 6-7 月数据缺失、候选池错位。未覆盖则删除内存缓存，走
                # 下方 cache.get 回源补齐。
                req_start = args[1] if len(args) > 1 else None
                req_end = args[2] if len(args) > 2 else None
                # 只在实时请求（end >= 今天）时检查过期；历史请求直接用内存数据
                _is_live_req = False
                if req_end is not None:
                    try:
                        _end_date = pd.Timestamp(req_end).date()
                        _is_live_req = _end_date >= pd.Timestamp.now().normalize().date()
                    except Exception:
                        _is_live_req = True
                if (DataCache._covers(mem, req_start, req_end)
                        and not (self.cache._is_stale(mem) and _is_live_req)):
                    return mem
            del self._daily_mem[cache_key]
            self._daily_ver += 1  # 日线内存有删除，money memo 旧版本键失效
        if self._offline:
            # 回测离线：本地缺失即视为无数据，不联网回源（避免 mootdx 选服务器
            # 联网超时卡死；缺数据的标的由策略侧容忍/跳过）。
            raise DataSourceError(f"离线模式本地缺失: {cache_key}")
        last_err = ""
        for name in self._priority():
            src = self.sources[name]
            try:
                if method in ("get_daily",):
                    code = args[0]
                    fetch_args = list(args)
                    df = self.cache.get(
                        "daily", f"{name}_{code}",
                        lambda s=src, a=fetch_args: getattr(s, method)(*a, **kwargs),
                        *(fetch_args[1:3] if len(fetch_args) > 1 else []),
                    )
                    if df is None or (hasattr(df, "empty") and df.empty):
                        raise DataSourceError(f"{name} 空数据")
                    # 与 preload 同口径：成交额单位归一为 money(元)、
                    # 成交量单位归一为 volume(股)
                    df = _ensure_money_yuan(df, name)
                    df = _ensure_volume_shares(df, name)
                    # 确保 index 是 DatetimeIndex（同 preload_daily 逻辑）
                    if not isinstance(df.index, pd.DatetimeIndex):
                        if "trade_date" in df.columns:
                            df.index = pd.to_datetime(df["trade_date"].astype(str))
                        elif "datetime" in df.columns:
                            df.index = pd.to_datetime(df["datetime"])
                        elif "timestamp" in df.columns:
                            df.index = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
                    self._daily_mem[cache_key] = df
                    self._daily_ver += 1  # 日线内存有写入，money memo 旧版本键失效
                    self._src_fail[name] = 0  # 该源成功取数，重置连续失败计数
                    return df
                result = getattr(src, method)(*args, **kwargs)
                self._daily_mem[cache_key] = result
                self._daily_ver += 1
                self._src_fail[name] = 0
                return result
            except Exception as e:
                last_err = f"{name}: {e}"
                if getattr(self, "_dbg_demote", False):
                    import traceback as _tb
                    print(f"[DBG-FETCH-FAIL] {name} {method} {args[:2]} :: {e}")
                    _tb.print_exc()
                self._src_fail[name] = self._src_fail.get(name, 0) + 1
                self._maybe_demote(name)
                continue
        raise DataSourceError(f"所有数据源失败: {last_err}")

    def _maybe_demote(self, name):
        """某数据源连续取数失败达阈值 -> 降到优先级末位并持久化到 .env。

        典型场景：某数据源对 ETF 日线始终返回空，前几次逐标的联网试探既慢又无意义；
        累计失败达阈值后自动把该源挪到最后，后续取数直接走 mootdx/astock，并把新
        顺序写回 .env，使后续运行也受益。已降级或本就在末位则跳过。
        """
        if name in self._demoted:
            return
        if self._src_fail.get(name, 0) < self._demote_threshold:
            return
        order = self._priority()
        if name not in order or order[-1] == name:
            self._demoted.add(name)
            return
        new_order = [n for n in order if n != name] + [name]
        self.set_priority(new_order)
        self._demoted.add(name)
        import logging
        logging.warning(
            "[DataManager] 数据源 %s 连续 %d 次取数失败，已降级到末位优先级并持久化: %s",
            name, self._src_fail[name], new_order,
        )
        print(f"[DataManager] {name} 连续 {self._src_fail[name]} 次取数失败，"
              f"已降级到末位优先级并写入 .env: {new_order}")

    def get_minute(self, code, end_date=None, start_date=None):
        """返回分钟线（真实优先 + 合成补齐），按 end_date/start_date 切片。

        策略内取数走**滑窗**：只加载/保留截至 ``end_date`` 最近 ``minute_lookback``
        天的分钟数据，避免每次按需取数都展开整段历史（即"每次只读约一周"）。
        ``start_date`` 仅用于最终切片，不改变加载窗口。
        """
        as_of = end_date or start_date
        try:
            df = self._ensure_minute_windowed(code, as_of)
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"[DataManager] 分钟数据加载异常: {code} 在 {as_of}: {e}"
            ) from e
        if df is None or (hasattr(df, "empty") and df.empty):
            # 缺失标的返回空 DataFrame（不 raise），让策略侧/feed 跳过而非中止回测。
            return pd.DataFrame()
        return self._slice_minute(df, end_date, start_date)

    def get_minute_feed(self, code, start, end):
        """桥接器时钟 feed 专用：加载并缓存**整段回测区间**的分钟线。

        与 :meth:`get_minute` 的滑窗不同，feed 需覆盖全部回测区间以驱动回放，
        因此按完整窗口加载（仅 1~2 只标的）。窗口上界用回测 end（而非 today）。

        H6a 修复：feed 改走 :meth:`_load_minute_merged` 真实 mootdx 分钟数据。
        H6c 修复：命中缓存前校验覆盖区间包含 [start, end]，覆盖不足重新合并，
        不再"先到先得"。
        """
        lo_ts = pd.Timestamp(start)
        hi_ts = pd.Timestamp(end)
        hi_eff = self._hi_eff(hi_ts)
        df = self._minute_cached(code, lo_ts, hi_eff)
        if df is None:
            # 加载整段回测区间 [start, end] 的分钟线（feed 仅 1~2 只标的）。
            df = self._load_minute_merged(code, lo_hi=(lo_ts, hi_ts))
            if df is not None and not df.empty:
                self._minute_mem[code] = df
                self._minute_cov[code] = (lo_ts, hi_eff)
        if df is None or (hasattr(df, "empty") and df.empty):
            # feed 缺失标的返回空 DataFrame，rqalpha 跳过该标的而非中止回测。
            return pd.DataFrame()
        return self._slice_minute(df, end, start)

    @staticmethod
    def _hi_eff(hi_ts):
        """窗口上界归一化：纯日期（午夜）上界扩到当日 15:00 以包含当天分钟
        bar（与 :meth:`_slice_minute` 口径一致）；带时分秒的精确上界不变。"""
        hi_ts = pd.Timestamp(hi_ts)
        if hi_ts == hi_ts.normalize():
            return hi_ts + pd.Timedelta(hours=15)
        return hi_ts

    def _minute_window(self, as_of=None, full=False, lo_hi=None):
        """计算分钟加载窗口 [lo_ts, hi_ts]。

        ``lo_hi`` 显式指定时优先（feed 整段区间）；``full`` 展开整个回测
        窗口；否则取截至 ``as_of`` 最近 ``minute_lookback`` 天（滑窗，默认）。
        """
        if lo_hi is not None:
            return pd.Timestamp(lo_hi[0]), pd.Timestamp(lo_hi[1])
        if self._minute_win:
            win_start0 = self._minute_win[0]
            win_end0 = self._minute_win[1]
        else:
            win_start0 = pd.Timestamp.today() - pd.Timedelta(days=400)
            win_end0 = pd.Timestamp.today()
        if full:
            return win_start0, win_end0
        hi_ts = pd.Timestamp(as_of) if as_of is not None else win_end0
        lo_ts = hi_ts - self.minute_lookback
        if lo_ts < win_start0:
            lo_ts = win_start0
        return lo_ts, hi_ts

    def _minute_cached(self, code, lo_ts, hi_eff):
        """覆盖校验命中：缓存帧的已覆盖区间完整包含 [lo_ts, hi_eff] 才返回，
        否则返回 None（调用方须重新加载/合并）。

        H6c 修复：原读侧命中即返回、不校验覆盖区间，同一标的的数据口径
        取决于访问顺序（先到先得）；加校验后同参数回测结果与访问顺序无关、
        可复现。
        """
        # 已知无数据的标的（指数等），跳过重复加载
        if code in self._minute_empty:
            return None
        df = self._minute_mem.get(code)
        if df is None or (hasattr(df, "empty") and df.empty):
            return None
        cov = self._minute_cov.get(code)
        if cov is None or cov[0] > lo_ts or cov[1] < hi_eff:
            return None
        return df

    def _ensure_minute_windowed(self, code, as_of):
        """确保 ``_minute_mem[code]`` 的缓存帧覆盖 as_of 对应滑窗，命中即返回。

        滑窗为 [as_of - minute_lookback, as_of]（下界不早于回测窗口起点）。
        H6c 修复：原实现"首次按需加载后永久复用"、命中不校验覆盖区间，同一
        标的数据口径取决于访问顺序；现按 _minute_cov 校验覆盖：覆盖才命中，
        否则重新按滑窗合并加载（窗口随 as_of 前移而滑动）。
        as_of 归一到日（上界扩到当日 15:00），同一交易日内的重复调用直接
        命中，不会逐 bar 重载；日内分钟级切片由调用方按精确 dt 自行截取。
        """
        # 已知无分钟数据的标的（指数等），直接跳过
        if code in self._minute_empty:
            return None
        if as_of is not None:
            as_of_ts = pd.Timestamp(as_of).normalize()
        else:
            win = self._minute_win
            as_of_ts = win[1] if win else pd.Timestamp.now().normalize()
        lo_ts, hi_ts = self._minute_window(as_of=as_of_ts, full=False)
        hi_eff = self._hi_eff(hi_ts)
        df = self._minute_cached(code, lo_ts, hi_eff)
        if df is not None:
            return df
        df = self._load_minute_merged(code, as_of=as_of_ts, full=False)
        if df is not None and not df.empty:
            self._minute_mem[code] = df
            self._minute_cov[code] = (lo_ts, hi_eff)
        else:
            self._minute_empty.add(code)
        return df

    def preload_minute_for_pool(self, codes, as_of=None):
        """批量预热分钟线缓存：把 codes 中所有标的的分钟数据加载到 _minute_mem。

        在策略构建好合并池后调用（如 `midday_routine` 后），使后续
        `get_price(..., frequency='1m')` 直接命中内存缓存，避免热路径联网/磁盘 IO。
        （重复定义合并：原 :301 与 :576 两份实现，后者覆盖前者；保留行为更完整
        的一份——``as_of`` 可缺省、逐标的失败仅告警。）
        H6c 修复：原实现对 ``code in _minute_mem`` 的标的直接跳过，滑窗前移后
        旧帧覆盖不足仍被复用；现交给 _ensure_minute_windowed 做覆盖校验，
        覆盖不足的标的会重新加载，保证池内帧始终覆盖 as_of。
        """
        as_of_ts = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
        logger.info("[DataManager] 开始预热分钟线缓存，标的数=%d", len(codes))
        loaded = 0
        for code in codes:
            try:
                # 滑窗加载（最近 minute_lookback 天），避免 full=True 触发全周期窗口
                # 超出 mootdx 实际覆盖范围。
                df = self._ensure_minute_windowed(code, as_of_ts)
                if df is not None and not (hasattr(df, "empty") and df.empty):
                    loaded += 1
            except Exception as e:
                logger.warning("预热分钟线失败 %s: %s", code, e)
        logger.info("[DataManager] 分钟线预热完成: 成功 %d/%d，已缓存 %d 只",
                    loaded, len(codes), len(self._minute_mem))

    def get_minute_price_at(self, code, dt):
        """取某标的截至 ``dt`` 的最后一分钟收盘价（供实时价/下单价，O(1) 切片）。

        回测中 ``_live_price``/``get_current_data`` 每个 bar 每个持仓都会调用，
        直接对滑窗内的 ``_minute_mem`` 切片取末值；数据不在窗内则按需按滑窗加载。
        返回 ``float``；无数据返回 ``None``。
        """
        dt_ts = pd.Timestamp(dt)
        df = self._minute_mem.get(code)
        cov = self._minute_cov.get(code)
        # H6c：dt 越出缓存帧覆盖区间（无论早晚）都重新按滑窗加载
        if (df is None or (hasattr(df, "empty") and df.empty) or cov is None
                or dt_ts > cov[1] or dt_ts < cov[0]):
            df = self._ensure_minute_windowed(code, dt_ts)
        if df is None or (hasattr(df, "empty") and df.empty):
            return None
        try:
            pos = df.index.searchsorted(dt_ts, side="right") - 1
            if pos < 0:
                return None
            return float(df["close"].iloc[pos])
        except Exception:
            return None

    @staticmethod
    def _adjust_for_splits(df):
        """检测分钟数据中的拆股/合股跳变并向前复权。

        隔夜缺口 >30% 视为拆股信号，计算分割比率，将跳变点之前的所有价格
        除以该比率（向前复权），确保跨拆股日的价格可比。
        """
        if df is None or df.empty or len(df) < 2:
            return df
        price_cols = ["open", "high", "low", "close"]
        has_price = any(c in df.columns for c in price_cols)
        if not has_price:
            return df
        ref = df["close"] if "close" in df.columns else df.get("open")
        if ref is None or ref.isna().all():
            return df
        dates = ref.index.normalize()
        unique_dates = dates.unique()
        if len(unique_dates) < 2:
            return df
        daily_last = ref.groupby(dates).last()
        daily_first = ref.groupby(dates).first()
        ratios = daily_first.values[1:] / daily_last.values[:-1]
        split_mask = np.isfinite(ratios) & (
            (ratios < 0.5) | (ratios > 2.0)
        )
        if not split_mask.any():
            return df
        split_dates = unique_dates[1:][split_mask]
        split_ratios = ratios[split_mask]
        result = df.copy()
        for split_date, ratio in zip(split_dates, split_ratios):
            if not np.isfinite(ratio) or ratio <= 0:
                continue
            mask = result.index < split_date
            for col in price_cols:
                if col in result.columns:
                    result.loc[mask, col] = result.loc[mask, col] / ratio
        return result

    def _load_minute_merged(self, code, all_min=None,
                            as_of=None, full=False, lo_hi=None):
        """仅使用真实 mootdx 分钟数据（无合成/插值兜底）。

        - 本地分钟缓存（``minute/real_<code>.parquet``）里的真实 1 分钟：查到多少用多少（基底）；
        - 本地数据**之后**的缺口 → 回源 mootdx 获取（见 ``_load_real_minute``）；
        - 无真实数据时返回 None。

        ``all_min`` 可选，传入 ``cache.get_all`` 的结果以跳过逐键查询。
        ``lo_hi`` 显式指定加载窗口 [lo, hi]（feed 整段区间用），优先级最高；
        否则 ``full`` 展开整段回测区间（feed 用），``full=False`` 只展开截至
        ``as_of`` 最近 ``minute_lookback`` 天（滑窗，默认）。

        H6b 修复：各层及合并结果统一裁剪到请求窗口 [lo, hi] 闭区间——原实现
        real_ 本地帧可延伸到窗口上界之外（回测末端之后），
        造成未来数据泄漏。
        """
        lo_ts, hi_ts = self._minute_window(as_of=as_of, full=full, lo_hi=lo_hi)
        hi_eff = self._hi_eff(hi_ts)
        use_real = getattr(self, "_use_real_minute", True)

        layers = []
        if use_real:
            real = self._load_real_minute(code, lo_ts, hi_ts, all_min)
            if real is not None and not real.empty:
                # H6b：real_ 本地帧可延伸到窗口之外（缓存随"今天"推移增长），
                # 先裁到 [lo, hi] 再并入，避免越界/未来数据进入合并结果。
                real = real.loc[(real.index >= lo_ts) & (real.index <= hi_eff)]
            if real is not None and not real.empty:
                layers.append(real)

        if not layers:
            # 无真实 mootdx 数据 → 返回 None。
            return None

        numeric_cols = ["open", "high", "low", "close", "volume", "money", "amount"]
        # 以所有层的索引并集为基底；后加入的层覆盖先前的层。
        union_idx = layers[0].index
        for layer in layers[1:]:
            union_idx = union_idx.union(layer.index)
        merged = pd.DataFrame(index=union_idx)
        for col in numeric_cols:
            if col not in merged.columns:
                merged[col] = float("nan")
        for layer in layers:
            for col in numeric_cols:
                if col in layer.columns:
                    merged.loc[layer.index, col] = layer[col]
        merged = merged.sort_index()
        # 拆股/合股调整：检测隔夜价格跳变（阈值30%），向前复权
        merged = self._adjust_for_splits(merged)
        # H6b：合并结果裁剪到请求窗口 [lo, hi] 闭区间（防越界/未来泄漏）。
        return merged.loc[(merged.index >= lo_ts) & (merged.index <= hi_eff)]

    def _load_real_minute(self, code, lo_ts, hi_ts, all_min):
        """缺口感知的真实分钟线获取（本地基底 + mootdx 仅补后续缺口）。

        本地命中 → 作为基底返回；仅当请求超出本地末端时才对"本地之后的缺口"
        回源 mootdx 并合并写回。这样不会用 mootdx 全量窗口覆盖掉本地较早的真实
        1 分钟——否则缓存会随"今天"推移不断缩水。

        请求整体早于本地最早日期（且本地无数据）→ mootdx 也取不到 → 返回 None。
        """
        real_key = f"real_{code}"
        if all_min is not None:
            local = all_min.get(real_key)
        else:
            local = self.cache.peek("minute", real_key)
        if local is not None and not local.empty:
            self._minute_real_cov.setdefault(
                code, (local.index.min(), local.index.max()))
            local_end = local.index.max()
            if hi_ts > local_end:
                # 仅补本地之后的缺口，避免覆盖本地较早的真实 1 分钟
                fresh = None
                if not self._offline:
                    # 优先从 DuckDB 补缺口
                    fresh = self._load_minute_from_duckdb(code, local_end + pd.Timedelta(minutes=1), hi_ts)
                if fresh is None or fresh.empty:
                    if not self._offline:
                        try:
                            gap_days = max((hi_ts - local_end).days, 1)
                            max_bars = min(gap_days * 360, 30000)
                            fresh = self.sources["mootdx"].get_minute(code, max_bars=max_bars)
                        except Exception:
                            fresh = None
                if fresh is not None and not fresh.empty:
                    fresh = fresh[fresh.index > local_end]
                    if not fresh.empty:
                        combined = pd.concat([local, fresh]).sort_index()
                        combined = combined[~combined.index.duplicated(keep="last")]
                        # C1 绝对约束：本地缺失段由 mootdx 真实获取后必须落盘，
                        # 下次回测直接命中本地，避免反复联网。仅真实 1m 落盘。
                        try:
                            self.cache.put("minute", real_key, combined)
                        except Exception as e:
                            import logging
                            logging.warning("[DataManager] real_ 落盘失败 %s: %s", code, e)
                        self._minute_real_cov[code] = (
                            combined.index.min(), combined.index.max())
                        return combined
            return local
        cov = self._minute_real_cov.get(code)
        if cov is not None and hi_ts < cov[0]:
            # 请求区间整体早于已知最早日期，回源也取不到 -> 跳过
            return None
        # DuckDB 分钟数据回退：本地 parquet 无数据时从 DuckDB 读取
        df = self._load_minute_from_duckdb(code, lo_ts, hi_ts)
        if df is not None and not df.empty:
            try:
                self.cache.put("minute", real_key, df)
            except Exception:
                pass
            self._minute_real_cov[code] = (df.index.min(), df.index.max())
            return df
        if self._offline:
            # 离线：完全缺失的标的不再联网回源，直接返回 None 由上层跳过
            return None
        try:
            # 无本地缓存：按请求窗口估算拉取量，避免默认30000全量拉取
            win_days = max((hi_ts - lo_ts).days, 1) if lo_ts and hi_ts else 15
            max_bars = min(win_days * 360, 30000)
            df = self.sources["mootdx"].get_minute(code, max_bars=max_bars)
        except Exception as e:
            import logging
            logging.warning("[DataManager] mootdx首次获取分钟数据失败 %s: %s", code, e)
            return None
        if df is not None and not df.empty:
            # C1 绝对约束：mootdx 真实获取的 1m 必须落盘，供后续回测复用。
            try:
                self.cache.put("minute", real_key, df)
            except Exception as e:
                import logging
                logging.warning("[DataManager] real_ 首次落盘失败 %s: %s", code, e)
            self._minute_real_cov[code] = (df.index.min(), df.index.max())
            return df
        return None

    def _load_minute_from_duckdb(self, code, lo_ts, hi_ts):
        """从 DuckDB kline_minute / kline_etf_minute 表读取分钟数据。"""
        try:
            sym = _jq_to_duckdb(code)
            duckdb_src = self.sources.get("duckdb")
            if duckdb_src is None:
                return None
            etf_syms = duckdb_src._get_etf_symbols()
            table = "kline_etf_minute" if sym in etf_syms else "kline_minute"
            conn = duckdb_src._get_conn()
            start_date = lo_ts.date() if lo_ts else None
            end_date = hi_ts.date() if hi_ts else None
            sql = f"SELECT datetime, open, high, low, close, volume, amount FROM {table} WHERE symbol = ?"
            params = [sym]
            if start_date:
                sql += " AND datetime::DATE >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND datetime::DATE <= ?"
                params.append(end_date)
            sql += " ORDER BY datetime"
            df_db = conn.execute(sql, params).pl().to_pandas()
            if df_db.empty:
                return None
            df_db["datetime"] = pd.to_datetime(df_db["datetime"])
            df_db = df_db.set_index("datetime")
            return df_db
        except Exception:
            return None

    @staticmethod
    def _slice_minute(df, end_date, start_date=None):
        end = (pd.Timestamp(end_date) if end_date
               else pd.Timestamp.now().normalize() + pd.Timedelta(hours=15))
        # 日期无时间分量（午夜）时，扩展到当日 15:00 以包含分钟 bar
        if end == end.normalize():
            end = end + pd.Timedelta(hours=15)
        # 用 searchsorted 直接定位，避免对整帧（数千行）做布尔掩膜遍历
        hi = df.index.searchsorted(end, side="right") - 1
        if hi < 0:
            return df.iloc[0:0]
        if start_date is not None:
            start = pd.Timestamp(start_date).normalize()
            lo = df.index.searchsorted(start, side="left")
            return df.iloc[max(0, lo):hi + 1]
        # 策略内调用：仅需近期分钟（当日量/近 N 分趋势），截取末 5 日
        lo = max(0, df.index.searchsorted(end - pd.Timedelta(days=5), side="left"))
        return df.iloc[lo:hi + 1]

    def get_daily_money_cached(self, codes, end_date, count=3):
        """从缓存直接取日线成交额，避免走 get_price 链路。

        返回 DataFrame(columns=['code','time','money'])：每只标的为
        ``end_date``（含）之前最近 ``count`` 个交易日的成交额明细，
        仅包含有数据的行。

        H7 修复：原实现先在 _build_money_full 里对每只的整段日线缓存
        ``tail(count)``、memo 化该截断帧，再按 ``time<=end_date`` 过滤——
        缓存随"今天"延伸后，回测期内任何过去的 end_date 都被 tail 截空
        （实测 4 个回测期 end_date 全返回 0 行），策略流动性过滤静默失效。
        现改为 memo 缓存**未截断**的全量明细（键含日线数据版本号），每次
        调用先按 end_date 过滤、再 per-code 取最近 count 行。

        性能：全量明细对固定 ``codes``+数据版本只算一次；后续每日调用仅
        过滤 + per-code tail（O(行数)），避免回测中每天对全市场 1600+ 只
        重复 to_datetime/过滤（原 ~1.1s/天 → 累计数分钟）。
        """
        memo_key = (tuple(sorted(codes)), self._daily_ver)
        full = self._money_memo.get(memo_key)
        if full is None:
            full = self._build_money_full(codes)
            # 旧版本/旧 codes 的 memo 已无引用价值，清掉避免随版本号累积
            self._money_memo.clear()
            self._money_memo[memo_key] = full
        if full is None or full.empty:
            return pd.DataFrame(columns=["code", "time", "money"])
        end_dt = pd.Timestamp(end_date).date()
        sub = full[full["time"].dt.date <= end_dt]
        if sub.empty:
            return pd.DataFrame(columns=["code", "time", "money"])
        # full 已按 (code, time) 升序，groupby tail 即每只 end_date 前最近 count 行
        return sub.groupby("code", sort=False).tail(count)

    def _build_money_full(self, codes):
        """构建全量成交额明细（不截断、不按 end_date 过滤），结果 memo 化复用。

        列：``code`` / ``time``(Timestamp) / ``money``，按 (code, time) 升序，
        仅含成交额>0 的行。H7 修复：此处不再 ``tail(count)`` 截断——截断推迟
        到 get_daily_money_cached 按 end_date 过滤之后 per-code 进行。
        """
        frames = []
        for code in codes:
            try:
                # C3：daily 必须真实，本地优先，本地全源缺失即回源并落盘。
                ddf = self._daily_mem.get("get_daily_" + code)
                # 覆盖检查：_daily_mem 可能来自 preload_daily() 加载的旧缓存
                if (ddf is not None and not (hasattr(ddf, "empty") and ddf.empty)
                        and self.cache._is_stale(ddf)):
                    ddf = None
                if ddf is None or (hasattr(ddf, "empty") and ddf.empty):
                    # peek 兜底顺序与 preload/fetch 一致复用 _priority()：
                    for src in self._priority():
                        cached = self.cache.peek("daily", f"{src}_{code}")
                        if cached is not None and not (hasattr(cached, "empty") and cached.empty):
                            ddf = _ensure_money_yuan(cached, src)
                            ddf = _ensure_volume_shares(ddf, src)
                            break
                if ddf is None or (hasattr(ddf, "empty") and ddf.empty):
                    # 本地全源缺失 -> 回源获取（C3：daily 必须真实，不得跳过）
                    try:
                        ddf = self.fetch("get_daily", code)
                    except Exception:
                        continue
                if ddf is None or (hasattr(ddf, "empty") and ddf.empty):
                    continue
                # daily_mem/preload 已预存 trade_dt 列（date 对象），直接复用。
                if "trade_dt" not in ddf.columns:
                    ddf = ddf.copy()
                    if "trade_date" in ddf.columns:
                        ddf["trade_dt"] = pd.to_datetime(
                            ddf["trade_date"].astype(str)).dt.date
                    elif "datetime" in ddf.columns:
                        ddf["trade_dt"] = pd.to_datetime(
                            ddf["datetime"]).dt.date
                    else:
                        continue
                money_col = ddf["money"] if "money" in ddf.columns else None
                amt_col = ddf["amount"] if "amount" in ddf.columns else None
                if money_col is None:
                    money_col = amt_col
                elif amt_col is not None:
                    money_col = money_col.fillna(amt_col)
                if money_col is None:
                    continue
                # 统一转 numpy 再组帧，避免 ddf 非 RangeIndex 时索引对齐错位
                sub = pd.DataFrame({
                    "time": pd.to_datetime(ddf["trade_dt"]).to_numpy(),
                    "money": money_col.astype(float).to_numpy(),
                })
                sub = sub[sub["money"] > 0]
                if sub.empty:
                    continue
                sub = sub.copy()
                sub["code"] = code
                frames.append(sub[["code", "time", "money"]])
            except Exception:
                continue
        if not frames:
            return pd.DataFrame(columns=["code", "time", "money"])
        full = pd.concat(frames, ignore_index=True)
        return full.sort_values(["code", "time"], kind="stable").reset_index(drop=True)

    def set_priority(self, order):
        CONFIG["DATASOURCE_PRIORITY"] = [o for o in order if o in SOURCES]
        self._write_env()

    def _write_env(self):
        env_path = os.path.join(REPO_ROOT, ".env")
        if not os.path.exists(env_path):
            with open(env_path, "w") as f:
                f.write("")
        set_key(env_path, "DATASOURCE_PRIORITY",
                ",".join(CONFIG["DATASOURCE_PRIORITY"]))

    def verify_token(self, token):
        return MootdxSource().test_connection()

    def list_sources(self):
        return [{"name": n, "priority": i, "available": True}
                for i, n in enumerate(self._priority())]
