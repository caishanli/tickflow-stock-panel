"""数据源优先级调度 + 自动降级 + 缓存管理器。"""

import datetime as _dt
import logging
import os
from collections import OrderedDict

import numpy as np
import pandas as pd
from dotenv import set_key

logger = logging.getLogger("jqengine.dm")

from .cache import DataCache
from .mootdx_src import MootdxSource
from .base import DataSourceError
from .network_source import NetworkSource
from ..config import CONFIG, REPO_ROOT

SOURCES = {"network": NetworkSource}

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


def _normalize_etf_volume_unit(df):
    """ETF 日线 volume 单位归一为「股」。

    上游数据个别标的（如 159939）volume 存成「手」（amount/(volume*close)≈100），
    绝大多数为「股」（≈1）。按 symbol 用成交额反推单位并换算，保证成交量/
    量比口径与聚宽一致。输入输出均为 Polars DataFrame（含 symbol/close/volume/amount）。
    """
    import polars as pl
    if df is None or df.is_empty() or "volume" not in df.columns:
        return df
    # 每股 symbol 取一行估算 ratio（amount/(volume*close)），>50 视为「手」
    ratio = (pl.col("amount") / (pl.col("volume") * pl.col("close"))).alias("_ratio")
    per_sym = df.group_by("symbol", maintain_order=True).agg(ratio.first())
    hand_syms = per_sym.filter(pl.col("_ratio") > 50).select("symbol")
    if hand_syms.is_empty():
        return df
    hand_set = set(hand_syms["symbol"].to_list())
    df = df.with_columns(
        pl.when(pl.col("symbol").is_in(list(hand_set)))
        .then(pl.col("volume") * 100)
        .otherwise(pl.col("volume"))
        .alias("volume")
    )
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

    def __init__(self, token=None, cache=None, minute_mem_cap=None, **kwargs):
        self.cache = cache or DataCache()
        self.client = kwargs.pop("client", None)
        if self.client is None:
            from app.quant.datasource.network_client import StockDataClient
            self.client = StockDataClient()
        self.sources = {k: v(self.client) for k, v in SOURCES.items()}
        # 网络单一数据源：把优先级固定为 network（避免旧 mootdx/astock 键
        # 参与 _maybe_demote / _priority 导致空列表）
        CONFIG["DATASOURCE_PRIORITY"] = ["network"]
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
        self._daily_preloaded = False  # preload_daily 幂等标志（分区数据已载入）
        # _daily_mem 数据版本号：每次写入/删除递增，money memo 键含版本据此失效
        self._daily_ver = 0
        self._money_memo = {}  # (codes_tuple, daily_ver) -> 全量成交额明细 DataFrame
        # 分钟线滑窗：回测中不持有整个回测区间的分钟数据，只保留最近 N 天，
        # 既省内存又避免每次按需加载都展开整段历史（见 get_minute / _ensure_minute_windowed）
        self.minute_lookback = pd.Timedelta(days=15)
        # 日线回看窗口：预加载时只扫最近 N 天分区（策略动量回看 ~65 日 + 余量）
        self._DAILY_LOOKBACK_DAYS = 400
        self._minute_real_cov = {}  # code -> (min_ts, max_ts) mootdx 真实分钟覆盖区间
        self._offline = False       # 回测离线模式：本地优先，缺失不联网回源
        # 数据源取数失败计数：同一源连续返回空/异常 N 次即自动降级到末位并持久化，
        # 避免低效源反复先试。
        self._src_fail = {}
        self._demote_threshold = 3
        self._demoted = set()
        # 前复权因子缓存（jq_code -> {trade_date: ex_factor}）：从
        # data/adj_factor_etf/all.parquet 惰性加载。分区日线为原始不复权价，
        # 聚宽 get_price(fq="pre") 对有拆分/分红事件的历史价按 ex_factor 折算
        # 对齐最新口径；此处复现该语义（仅作用于日线价格列，分钟/当前价保持
        # 真实价，与聚宽 use_real_price 行为一致）。
        self._adj_factor = None  # 惰性：None=未加载，{} = 已加载但空
        self._adj_events_cache = None  # 惰性：从因子表重建的除权事件

    def _adj_factor_map(self) -> dict[str, dict]:
        """加载前复权因子表：{jq_code: {trade_date(date): ex_factor}}。"""
        if self._adj_factor is not None:
            return self._adj_factor
        self._adj_factor = {}
        try:
            df = self.client.get_adj_factors()
            if df is not None and not df.empty:
                for row in df.to_dict("records"):
                    jq = self._to_jq_code(row["symbol"])
                    self._adj_factor.setdefault(jq, {})[pd.Timestamp(row["trade_date"])] = float(row["ex_factor"])
        except Exception as e:
            logger.warning("[DataManager] adj_factor 加载失败: %s", e)
        return self._adj_factor

    def _apply_qfq(self, pdf: pd.DataFrame, jq: str, cutoff=None) -> pd.DataFrame:
        """对日线帧价格列应用前复权因子（有拆分/分红的标的）。

        聚宽 get_price(fq="pre") 为**动态前复权**：在决策日 D 调用时，只用
        事件日 <= D 的除权事件折算历史价，未来事件不参与（避免 515880 在
        7/6 拆分前被 7/6 因子影响）。``cutoff`` 为决策日（None 时用全量
        事件，即"最新口径"）。无事件/无因子表时原样返回。
        """
        events = self._adj_events().get(jq)
        if not events:
            return pdf
        if cutoff is not None:
            cutoff = pd.Timestamp(cutoff).normalize()
            events = [e for e in events if e[0] <= cutoff]
            if not events:
                return pdf
        dates = pd.to_datetime(pdf["trade_dt"]).values
        factors = np.ones(len(dates), dtype=float)
        for ex_dt, f in events:
            mask = dates < ex_dt
            factors[mask] *= f
        if np.allclose(factors, 1.0):
            return pdf
        pdf = pdf.copy()
        for col in ("open", "high", "low", "close"):
            if col in pdf.columns:
                pdf[col] = pdf[col].astype(float) * factors
        return pdf

    def _adj_events(self) -> dict[str, list[tuple[pd.Timestamp, float]]]:
        """从累计因子表重建除权事件：{jq_code: [(ex_date, factor), ...]}。

        累计因子表每日期 ex_factor = 所有 > 该日事件因子的连乘。相邻日期
        因子跳变即暴露事件：f = ex_factor(prev) / ex_factor(curr)，事件日 =
        curr。示例：159667 6/9=0.333, 6/10=1.0 → 事件 (6/10, 0.333/1.0)。

        聚宽 pre/none 比值含 4 位价格精度噪声（±0.1% 级微小波动），需过滤：
        仅保留因子明显偏离 1 的真实除权事件（阈值 10%），忽略噪声。
        """
        if self._adj_events_cache is not None:
            return self._adj_events_cache
        self._adj_events_cache = {}
        fmap = self._adj_factor_map()
        for jq, m in fmap.items():
            items = sorted(m.items())
            if len(items) < 2:
                continue
            events = []
            for i in range(1, len(items)):
                prev_dt, prev_f = items[i - 1]
                cur_dt, cur_f = items[i]
                if cur_f != prev_f:
                    f = prev_f / cur_f if cur_f else 0.0
                    # 阈值：除权事件因子偏离 1 至少 10%（0.1 < f < 0.9 或
                    # f > 1.1），过滤聚宽价格精度噪声产生的伪事件
                    if (f < 0.9 or f > 1.1) and 0.0 < f < 10.0:
                        events.append((cur_dt, f))
            if events:
                self._adj_events_cache[jq] = events
        return self._adj_events_cache

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

    # ---- 按日分区 Parquet 读取（分钟/日线统一按日期分文件） ----
    # 分区根目录在项目根 data/ 下（与 tickflow 数据层同根）：
    #   data/kline_etf_minute/date=YYYY-MM-DD/data_*.parquet（ETF 分钟）
    #   data/kline_minute/date=YYYY-MM-DD/data_*.parquet      （股票分钟）
    #   data/kline_daily/date=YYYY-MM-DD/data_*.parquet       （日线）
    # 分区内列：symbol（.SH/.SZ）、datetime/date、OHLCV、volume、amount。
    # 通过环境变量 PARTITION_DATA_ROOT 可覆盖根目录（默认项目根 data/）。
    @staticmethod
    def _partition_root() -> str:
        root = os.getenv("PARTITION_DATA_ROOT", "")
        if root:
            return root
        return os.path.join(os.path.dirname(REPO_ROOT), "data")

    @classmethod
    def _partition_dates(cls, subdir: str) -> list[str]:
        """返回某分钟/日线子目录下全部 date=YYYY-MM-DD 分区目录名（升序）。"""
        d = os.path.join(cls._partition_root(), subdir)
        if not os.path.isdir(d):
            return []
        out = []
        for name in os.listdir(d):
            if name.startswith("date="):
                out.append(name)
        return sorted(out)

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

    def preload_daily(self, force: bool = False):
        if getattr(self, "_daily_preloaded", False) and not force:
            return
        try:
            from_part = self.client.preload_daily(
                lookback_days=self._DAILY_LOOKBACK_DAYS,
                asof=pd.Timestamp.now().normalize().date() - pd.Timedelta(days=1))
            if from_part:
                for jq, df in from_part.items():
                    df = _ensure_money_yuan(df, "network")
                    df = _ensure_volume_shares(df, "network")
                    self._daily_mem[f"get_daily_{jq}"] = df
                self._daily_ver += 1
                self._daily_preloaded = True
                return
        except Exception as e:
            logger.warning("[DataManager] preload_daily 网络取数失败: %s", e)
        print("[preload] 日线缓存为空")

    def _load_daily_from_partitions(self, asof: pd.Timestamp) -> dict[str, pd.DataFrame]:
        """从按日分区 Parquet（data/kline_daily/ 与 data/kline_etf_daily/）加载全市场日线。

        返回 {jq_code(.XSHG/.XSHE): DataFrame}，帧为 DatetimeIndex 索引、
        含 trade_dt/money/volume 列（与 preload_daily 内存帧同口径）。
        分区不存在/为空时返回 {}，调用方回退原缓存路径。

        用 Polars 惰性扫描全部 date= 分区（hive 分区自动识别），一次 collect
        后按 symbol 分组转 pandas，避免 pandas 逐文件读数百分区（极慢）。
        ETF 日线（kline_etf_daily）与股票日线（kline_daily）合并加载，
        二者 schema 一致。

        volume 单位：A股日线（kline_daily）存盘为「手」，读取时 ×100 归一
        为「股」；ETF 日线（kline_etf_daily）存盘即为「股」，不换算。
        asof 过滤用 date 列（hive_partitioning 会把分区键 date 暴露为列，
        文件内部无 date 列的分区也能正确过滤）。
        """
        roots = [(os.path.join(self._partition_root(), "kline_daily"), True),
                 (os.path.join(self._partition_root(), "kline_etf_daily"), False)]
        roots = [(r, a) for r, a in roots if os.path.isdir(r)]
        if not roots:
            return {}
        try:
            import glob as _glob
            import polars as pl
            # 只扫描「回看窗口内」的日期分区：策略动量回看 ~65 日、流动性 3 日，
            # 全历史扫描（kline_etf_daily 自 2005 年、5211 分区）纯属浪费——
            # 从目录名反推下限，只读最近 lookback 天，回测预加载从 ~17s 降到秒级。
            from datetime import timedelta
            _limit_date = None
            if asof is not None:
                _limit_date = (pd.Timestamp(asof).normalize()
                               - timedelta(days=self._DAILY_LOOKBACK_DAYS))
            elif getattr(self, "_minute_win", None) is not None:
                _limit_date = (pd.Timestamp(self._minute_win[0]).normalize()
                               - timedelta(days=self._DAILY_LOOKBACK_DAYS))
            else:
                # 无窗口（首次 preload 在 set_minute_window 之前）：也限制最近
                # N 天，避免全历史扫描（kline_etf_daily 自 2005 年 5211 分区）。
                _limit_date = (pd.Timestamp.now().normalize()
                               - timedelta(days=self._DAILY_LOOKBACK_DAYS))
            parts = []
            for root, is_a_stock in roots:
                scan_paths = []
                if _limit_date is not None:
                    lo_s = _limit_date.strftime("%Y-%m-%d")
                    for name in os.listdir(root):
                        if not name.startswith("date="):
                            continue
                        if name[len("date="):] >= lo_s:
                            scan_paths.extend(_glob.glob(
                                os.path.join(root, name, "*.parquet")))
                    if not scan_paths:
                        continue
                lf = pl.scan_parquet(
                    scan_paths if scan_paths
                    else os.path.join(root, "**", "*.parquet"),
                    hive_partitioning=True,
                )
                if asof is not None:
                    lf = lf.filter(pl.col("date") <= pd.Timestamp(asof).date())
                if is_a_stock:
                    lf = lf.with_columns((pl.col("volume") * 100).alias("volume"))
                parts.append(lf.select(["symbol", "date", "open", "high", "low",
                                        "close", "volume", "amount"]))
            df = pl.concat(parts).collect()
            if df.is_empty():
                return {}
            # ETF 日线 volume 单位归一：上游个别标的（如 159939）volume 存成
            # 「手」而绝大多数是「股」（amount/(volume*close)≈100 vs ≈1）。
            # 按 symbol 检测并换算为股，保证成交量/量比口径与聚宽一致。
            df = _normalize_etf_volume_unit(df)
            # 一次 to_pandas 拿全市场宽表，再按 symbol 切片，避免 7000+ 次
            # 独立 to_pandas/set_index 转换（回测预热头号热点之一）
            df = df.with_columns(
                pl.col("date").cast(pl.Datetime).dt.date().alias("_trade_dt"),
            )
            all_pd = df.to_pandas()
            all_pd = all_pd.set_index(pd.to_datetime(all_pd["date"]))
            all_pd.index.name = None
            out: dict[str, pd.DataFrame] = {}
            total_rows = 0
            for sym, g in all_pd.groupby("symbol"):
                jq = self._to_jq_code(sym)
                pdf = g.drop(columns=["date", "symbol", "_trade_dt"]).copy()
                pdf["trade_dt"] = g["_trade_dt"].values
                pdf = _ensure_money_yuan(pdf, "partition")
                pdf = _ensure_volume_shares(pdf, "partition")
                out[jq] = pdf
                total_rows += len(pdf)
            print(f"[preload] 日线分区: {len(out)} 只, {total_rows} 行")
            return out
        except Exception as e:
            logger.warning("[DataManager] 日线分区读取失败: %s", e)
            return {}

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
            try:
                if method in ("get_daily",):
                    code = args[0]
                    # fetch("get_daily", code) 单参（_build_money_full 等）缺
                    # start/end → 补全量窗口，避免 NetworkSource.get_daily 缺参抛
                    # 错导致全球池成交额过滤静默失效（模拟盘补跑 _daily_mem 空时
                    # 触发）。显式传了日期则原样透传。
                    _fetch_args = list(args)
                    if len(_fetch_args) < 3:
                        _fetch_args += ["2000-01-01", _dt.datetime.now().strftime("%Y-%m-%d")]
                        _fetch_args = _fetch_args[:3]
                    df = getattr(self.sources["network"], method)(*_fetch_args, **kwargs)
                    if df is None or (hasattr(df, "empty") and df.empty):
                        raise DataSourceError(f"network 空数据")
                    # 与旧分区路径同口径：网络帧补 money/volume 列（幂等，列在则跳过）
                    df = _ensure_money_yuan(df, "network")
                    df = _ensure_volume_shares(df, "network")
                    self._daily_mem[cache_key] = df
                    self._daily_ver += 1
                    self._src_fail["network"] = 0
                    return df
                result = getattr(self.sources["network"], method)(*args, **kwargs)
                self._daily_mem[cache_key] = result
                self._daily_ver += 1
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

    def _load_minute_pool_from_partitions(self, codes, lo_ts, hi_ts):
        if not codes:
            return {}
        try:
            return self.client.get_minute_pool(codes, lo_ts, hi_ts)
        except Exception as e:
            logger.warning("[DataManager] 分钟池网络取数失败: %s", e)
            return {}

    def preload_minute_for_pool(self, codes, as_of=None):
        """批量预热分钟线缓存：把 codes 中所有标的的分钟数据加载到 _minute_mem。

        在策略构建好合并池后调用（如 `midday_routine` 后），使后续
        `get_price(..., frequency='1m')` 直接命中内存缓存，避免热路径联网/磁盘 IO。
        （重复定义合并：原 :301 与 :576 两份实现，后者覆盖前者；保留行为更完整
        的一份——``as_of`` 可缺省、逐标的失败仅告警。）
        H6c 修复：原实现对 ``code in _minute_mem`` 的标的直接跳过，滑窗前移后
        旧帧覆盖不足仍被复用；现交给 _ensure_minute_windowed 做覆盖校验，
        覆盖不足的标的会重新加载，保证池内帧始终覆盖 as_of。

        Perf：同一 as_of 下 pool 内标的多达数百只，逐标的 scan_parquet 各自
        全目录扫描是回测头号热点（620 次 collect ≈ 90s）。改为一次批量
        scan（_load_minute_pool_from_partitions）取回所有标的分钟，再逐标的
        覆盖校验/裁剪填入 _minute_mem；未命中/覆盖不足的标的后回退单标的加载。
        """
        as_of_ts = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
        logger.info("[DataManager] 开始预热分钟线缓存，标的数=%d", len(codes))
        # 回测（已 set_minute_window）加载整段回测窗口并去重：池内标的一次性把
        # 全窗口分钟取回，JqDataSource 的 get_minute_feed 直接命中 _minute_mem，
        # 不再逐标的按全窗口联网回源（T17 实测 291 次单标的 1m 全窗口 ≈ 68s）。
        # 实时/模拟盘（无 _minute_win）保持滑窗语义，帧随 as_of 前移由
        # _ensure_minute_windowed 覆盖校验自愈。
        full = bool(getattr(self, "_minute_win", None))
        lo_ts, hi_ts = self._minute_window(as_of=as_of_ts, full=full)
        hi_eff = self._hi_eff(hi_ts)
        loaded = 0
        # 过滤已知无分钟的标的 + 已覆盖标点（同日重复预热直接跳过，只批新入池）
        todo = [c for c in codes
                if c not in self._minute_empty
                and self._minute_cached(c, lo_ts, hi_eff) is None]
        if not todo:
            logger.info("[DataManager] 分钟线预热完成: 成功 %d/%d，已缓存 %d 只",
                        len(codes) - len(todo), len(codes), len(self._minute_mem))
            return
        batch = self._load_minute_pool_from_partitions(todo, lo_ts, hi_ts)
        for code in todo:
            try:
                df = batch.get(code)
                if df is not None and not df.empty:
                    df = df.loc[(df.index >= lo_ts) & (df.index <= hi_eff)]
                    df = self._adjust_for_splits(df)
                    df = df.loc[(df.index >= lo_ts) & (df.index <= hi_eff)]
                    if not df.empty:
                        self._minute_mem[code] = df
                        self._minute_cov[code] = (lo_ts, hi_eff)
                        loaded += 1
                        continue
                # 批量缺失 → 回退单标的路径（保 _minute_empty 语义）
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
            # 向前复权到最新口径：拆股/合股后，历史价应乘以 ratio
            # （ratio = 次日首价/前日末价，如 3.08x 拆分 ratio=0.325 → 历史价 ×0.325
            # 缩小到与最新价连续）。原实现 `col / ratio` 反向放大（×3.08），
            # 把动量窗口价格整体抬升，污染动量分（对齐 bug：159667 拆分类）。
            for col in price_cols:
                if col in result.columns:
                    result.loc[mask, col] = result.loc[mask, col] * ratio
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

    def _load_minute_from_partitions(self, code, lo_ts, hi_ts):
        try:
            out = self.client.get_price(code, frequency="1m",
                                        start_date=str(lo_ts) if lo_ts is not None else None,
                                        end_date=str(hi_ts) if hi_ts is not None else None)
            return out.get(code)
        except Exception as e:
            logger.warning("[DataManager] 分钟网络取数失败 %s: %s", code, e)
            return None

    def _load_real_minute(self, code, lo_ts, hi_ts, all_min):
        df = self._load_minute_from_partitions(code, lo_ts, hi_ts)
        if df is not None and not df.empty:
            self._minute_real_cov[code] = (df.index.min(), df.index.max())
            return df
        if getattr(self, "_offline_missing_warn", False):
            logger.warning("[DataManager] 离线分钟缺失（网络无数据）: %s", code)
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
                # 覆盖检查：_daily_mem 可能来自 preload_daily() 加载的旧缓存。
                # 离线回测中数据静态、不会过期，跳过 _is_stale 的 pandas 日期
                # 计算（全市场 1600+ 只 × 每次 3.5s，回测流动性过滤的头号热点）。
                if (ddf is not None and not (hasattr(ddf, "empty") and ddf.empty)
                        and not self._offline and self.cache._is_stale(ddf)):
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
                # 统一转 numpy 再组帧，避免 ddf 非 RangeIndex 时索引对齐错位。
                # trade_dt 是 datetime 列时直接取 .to_numpy()，跳过重复
                # pd.to_datetime（1600+ 只 × 11 次调用，回测流动性热点）。
                _td = ddf["trade_dt"]
                if _td.dtype.kind == "M":
                    _td_arr = _td.to_numpy()
                else:
                    _td_arr = pd.to_datetime(_td).to_numpy()
                money_arr = money_col.astype(float).to_numpy()
                mask = money_arr > 0
                if not mask.any():
                    continue
                # 用 numpy 数组直接拼，避免逐 code 建 DataFrame + concat（130万行）
                n = int(mask.sum())
                frames.append({
                    "code": np.full(n, code, dtype=object),
                    "time": _td_arr[mask],
                    "money": money_arr[mask],
                })
            except Exception:
                continue
        if not frames:
            return pd.DataFrame(columns=["code", "time", "money"])
        full = pd.DataFrame({
            "code": np.concatenate([f["code"] for f in frames]),
            "time": np.concatenate([f["time"] for f in frames]),
            "money": np.concatenate([f["money"] for f in frames]),
        })
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
