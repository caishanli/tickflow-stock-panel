"""数据源优先级调度 + 自动降级 + 缓存管理器。"""

import logging
import os
from collections import OrderedDict

import pandas as pd
from dotenv import set_key

logger = logging.getLogger("jqengine.dm")

from .cache import DataCache
from .tushare_src import TushareSource
from .mootdx_src import MootdxSource
from .astock_src import AStockSource
from .baostock_src import BaostockSource, interpolate_5min_to_1min
from .minute_synth import SyntheticMinuteSource
from .base import DataSourceError
from ..config import CONFIG, REPO_ROOT

SOURCES = {"tushare": TushareSource, "mootdx": MootdxSource, "astock": AStockSource}

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

    各数据源 amount 单位不一：tushare pro.daily/fund_daily 的 ``amount`` 是
    **千元**（实测 510300 amount=4,039,507 ≈ close×vol(手)×100/1000，与聚宽
    全市场成交额口径一致），mootdx/baostock/astock 为元；mootdx 源已自带
    money(元)（见 mootdx_src.get_daily 的 amount→money 映射）。未归一时直接
    把 amount 当元用会让全市场成交额聚合小 1000 倍（实测本地 41.7 亿 vs 聚宽
    4967 亿），流动性门槛/池过滤全面失真。
    """
    if df is None or getattr(df, "empty", True):
        return df
    if "money" in df.columns:
        return df
    if "amount" not in df.columns:
        return df
    factor = 1000.0 if src_name == "tushare" else 1.0
    df = df.copy()
    df["money"] = pd.to_numeric(df["amount"], errors="coerce") * factor
    return df


def _ensure_volume_shares(df, src_name):
    """保证日线帧带 ``volume`` 列（单位：股）。

    与 :func:`_ensure_money_yuan` 同模式：tushare pro.daily/fund_daily 的
    ``vol`` 单位是**手**（1 手=100 股），归一为 ``volume``(股)（保留原
    ``vol`` 列）；mootdx 源内已 vol×100→volume(股)（见
    mootdx_src.get_daily），baostock/astock 的 volume 单位即为股（astock
    实测小 ETF 约 5e7 量级），已有 ``volume`` 列的帧不动。
    """
    if df is None or getattr(df, "empty", True):
        return df
    if "volume" in df.columns:
        return df
    if "vol" not in df.columns:
        return df
    factor = 100.0 if src_name == "tushare" else 1.0
    df = df.copy()
    df["volume"] = pd.to_numeric(df["vol"], errors="coerce") * factor
    return df


def _infer_adj(df, src_name):
    """推断日线帧的复权口径：优先读 ``df.attrs["adj"]``（各源 get_daily
    已标注）；旧缓存帧（pickle/parquet）没有 attrs，按源名回退推断——
    tushare 请求 adj="qfq" 按 "qfq"、mootdx 通达信原始价按 "raw"、
    baostock 日线 adjustflag="1" 按 "qfq"、其余（如 astock）按 "unknown"。
    """
    adj = getattr(df, "attrs", None)
    adj = adj.get("adj") if adj else None
    if adj:
        return adj
    return {"tushare": "qfq", "mootdx": "raw",
            "baostock": "qfq"}.get(src_name, "unknown")


def _pick_daily_frame(candidates):
    """同一代码多源缓存并存时的选帧：按优先级取第一个非 raw 帧，全 raw 才用 raw。

    背景：mootdx 为通达信原始不复权价（raw），tushare/baostock 为前复权
    （qfq）；若只按源优先级选帧，同一回测内价格帧可能来自 qfq 源而 money
    帧来自 raw 源，复权口径混乱。``candidates`` 为 ``(pri, src_name, key,
    df)`` 元组、已按优先级升序；"非 raw" 含 qfq/unknown（unknown 可能是
    前复权，raw 一定不是）。返回选中的元组；无候选返回 None。
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
        tok = token if token is not None else CONFIG["TUSHARE_TOKEN"]
        self.sources = {
            k: (v(token=tok) if k == "tushare" else v())
            for k, v in SOURCES.items()
        }
        # 历史分钟线：优先走真实源（mootdx 分页回看），回退到日线合成的确定性分钟源
        self.minute_source = SyntheticMinuteSource(
            lambda code, start, end: self.fetch("get_daily", code, start, end)
        )
        # baostock 仅作分钟中间层（5分钟插值），不进入日线优先级链
        self.sources["baostock"] = BaostockSource()
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
        # 避免 tushare 对 ETF 永远返回空却每次都先试（慢且无意义）。
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
        self.minute_source.window = (pd.Timestamp(start), pd.Timestamp(end))
        # clear() 而非重新赋值：保持 _minute_mem 对象身份（LRU 实例）不变
        self._minute_mem.clear()
        self._minute_cov.clear()

    @staticmethod
    def _to_jq_code(code):
        """tushare代码(.SZ/.SH) -> JQ代码(.XSHE/.XSHG)。"""
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
        all_5min = self.cache.get_all("5min")
        win = self.minute_source.window
        count = 0
        for code in codes:
            try:
                df = self._load_minute_merged(code, all_min, all_5min, full=True)
                if df is not None and not df.empty:
                    self._minute_mem[code] = df
                    if win:
                        # 记录实际覆盖区间（上界归一到当日 15:00），供命中校验
                        self._minute_cov[code] = (win[0], self._hi_eff(win[1]))
                    count += 1
            except Exception:
                continue
        print(f"[preload] 分钟线: {count}/{len(codes)} 只")

    def preload_daily(self):
        """一次性加载全部日线缓存到内存，避免回测中逐文件读取。

        一条查询把整库日线缓存读入内存，同一代码多源缓存并存时按数据源
        优先级 + 复权口径选帧（:func:`_pick_daily_frame`：优先非 raw 的
        前复权帧，全 raw 才用 raw，保证回测内复权口径尽量统一），存入
        _daily_mem。
        """
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
            # 跳过过期日线：preload 不把过期数据读入内存，迫使 fetch 回源刷新
            if self.cache._is_stale(df):
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
            if "trade_dt" not in df.columns and "trade_date" in df.columns:
                df = df.copy()
                df["trade_dt"] = pd.to_datetime(
                    df["trade_date"].astype(str)).dt.date
            # 成交额单位归一（tushare amount 为千元 → 元），下游
            # get_daily_money_cached / total_turnover 统一用 money(元)
            df = _ensure_money_yuan(df, src_name)
            # 成交量单位归一（tushare vol 为手 ×100 → 股），与 money 同模式
            df = _ensure_volume_shares(df, src_name)
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
        # cache.get 会调 loader 回源（mootdx/tushare）并落盘。回测中
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
                if (DataCache._covers(mem, req_start, req_end)
                        and not self.cache._is_stale(mem)):
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
                    df = self.cache.get(
                        "daily", f"{name}_{code}",
                        lambda s=src: getattr(s, method)(*args, **kwargs),
                        *(args[1:3] if len(args) > 1 else []),
                    )
                    if df is None or (hasattr(df, "empty") and df.empty):
                        raise DataSourceError(f"{name} 空数据")
                    # 与 preload 同口径：成交额单位归一为 money(元)、
                    # 成交量单位归一为 volume(股)
                    df = _ensure_money_yuan(df, name)
                    df = _ensure_volume_shares(df, name)
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

        典型场景：tushare 对 ETF 日线始终返回空，前几次逐标的联网试探既慢又无意义；
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
        因此按完整窗口加载（仅 1~2 只标的）。窗口上界用回测 end（而非 today），
        避免超出本地 5min 缓存覆盖而被迫回源 baostock 网络。

        H6a 修复：feed 改走 :meth:`_load_minute_merged` 三源合并（real_ 真实
        1 分钟基底 + baostock 5 分钟插值补缺口），兑现 rqalpha_bridge "回测
        使用真实 1 分钟数据" 的承诺；原实现只走 _load_baostock_minute，整段
        区间都是 5 分钟插值，``_use_real_minute`` 开关在 feed 路径完全失效。
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
        if self.minute_source.window:
            win_start0 = self.minute_source.window[0]
            win_end0 = self.minute_source.window[1]
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
        标的的数据口径取决于访问顺序；现按 _minute_cov 校验覆盖：覆盖才命中，
        否则重新按滑窗合并加载（窗口随 as_of 前移而滑动）。
        as_of 归一到日（上界扩到当日 15:00），同一交易日内的重复调用直接
        命中，不会逐 bar 重载；日内分钟级切片由调用方按精确 dt 自行截取。
        """
        if as_of is not None:
            as_of_ts = pd.Timestamp(as_of).normalize()
        else:
            win = self.minute_source.window
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
                # 超出本地 5min 缓存覆盖范围而被迫回源 baostock 网络。
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

    def _load_minute_merged(self, code, all_min=None, all_5min=None,
                            as_of=None, full=False, lo_hi=None):
        """三源合并（缺口感知）：本地基底 + mootdx 补后续缺口 + baostock 补前序缺口。

        - 本地分钟缓存（``minute/real_<code>.parquet``）里的真实 1 分钟：查到多少用多少（基底）；
        - 本地数据**之后**的缺口 → 回源 mootdx 获取（见 ``_load_real_minute``）；
        - 本地数据**之前**的缺口 → baostock 5 分钟插值成 1 分钟补齐；
        - 5 分钟线与 1 分钟线在缺口边界重叠的时间点：以 5 分钟线为准；
        - 三源皆无 → 日线合成兜底（仅兜底，确定性）。

        ``all_min`` / ``all_5min`` 可选，传入 ``cache.get_all`` 的结果以跳过逐键查询。
        ``lo_hi`` 显式指定加载窗口 [lo, hi]（feed 整段区间用），优先级最高；
        否则 ``full`` 展开整段回测区间（feed 用），``full=False`` 只展开截至
        ``as_of`` 最近 ``minute_lookback`` 天（滑窗，默认）。

        H6b 修复：各层及合并结果统一裁剪到请求窗口 [lo, hi] 闭区间——原实现
        real_ 本地帧与 baostock 插值帧都可延伸到窗口上界之外（回测末端之后），
        造成未来数据泄漏。
        """
        lo_ts, hi_ts = self._minute_window(as_of=as_of, full=full, lo_hi=lo_hi)
        hi_eff = self._hi_eff(hi_ts)
        use_real = getattr(self, "_use_real_minute", True)

        layers = []
        real_start = None
        if use_real:
            real = self._load_real_minute(code, lo_ts, hi_ts, all_min)
            if real is not None and not real.empty:
                # H6b：real_ 本地帧可延伸到窗口之外（缓存随"今天"推移增长），
                # 先裁到 [lo, hi] 再并入，避免越界/未来数据进入合并结果。
                real = real.loc[(real.index >= lo_ts) & (real.index <= hi_eff)]
            if real is not None and not real.empty:
                layers.append(real)
                real_start = real.index.min()

        # 本地/真实数据“之前”的缺口：baostock 5 分钟插值补齐（只补缺口，
        # 不覆盖真实段，避免无谓联网也避免用插值覆盖真实 1 分钟）。
        if real_start is None or lo_ts < real_start:
            try:
                bs_hi = real_start if real_start is not None else hi_ts
                baostock = self._load_baostock_minute(code, lo_ts, bs_hi, all_5min)
                if baostock is not None and not baostock.empty:
                    layers.append(baostock)
            except Exception as e:
                import logging
                logging.warning("[DataManager] baostock分钟数据获取失败 %s: %s", code, e)

        if not layers:
            # 三源皆无（mootdx/baostock 均失败）→ 最后兜底：日线合成分钟。
            # 仅内存使用、绝不落盘（C2：非真实源数据不写库）。确定性、可复现。
            try:
                synth = self.minute_source.get_minute(code, hi_ts, lo_ts)
                if synth is not None and not synth.empty:
                    # H6b：合成兜底同样不得越出请求窗口
                    return synth.loc[(synth.index >= lo_ts) & (synth.index <= hi_eff)]
            except Exception as e:
                import logging
                logging.warning("[DataManager] 日线合成兜底失败 %s: %s", code, e)
            # 三源皆缺：返回 None（不 raise），让上层 feed 跳过该标的而非中止回测。
            return None

        numeric_cols = ["open", "high", "low", "close", "volume", "money", "amount"]
        # 以所有层的索引并集为基底；后加入的层（baostock 5 分钟）覆盖先前的层，
        # 即缺口边界的重叠点以 5 分钟线为准。
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
        # H6b：合并结果裁剪到请求窗口 [lo, hi] 闭区间（防越界/未来泄漏）。
        return merged.loc[(merged.index >= lo_ts) & (merged.index <= hi_eff)]

    def _load_baostock_minute(self, code, lo_ts, hi_ts, all_5min):
        """baostock 5 分钟线的本地优先获取 + 运行时插值。

        本地 5min 缓存命中且覆盖请求区间 → 直接用；否则从 baostock 回源
        （仅拉取缺失区间），与已缓存区间合并后落盘本地 5min 缓存；插值出的 1 分钟
        只在本进程内使用、**不落盘**（避免数据库膨胀）。

        注意: 缓存按 code 单键存储，若只缓存过局部区间(如某次只取了 4 月)，
        后续请求早期区间时必须重新回源并合并，否则会拿错区间的数据，导致
        早期 1 分钟缺口无法被 baostock 补齐。

        H6b 修复：所有返回路径在插值前统一把 5 分钟帧裁到请求窗口 [lo, hi]
        闭区间。原离线兜底只卡下界（``cached.index >= lo_ts``），导致：
        1) merged 路径中 baostock 层延伸进 real_ 真实段，把重叠区间覆盖成
        插值（实测预热帧中 real 段只剩最后 6 个交易日）；
        2) 帧可延伸到请求上界之外（回测末端之后），泄漏未来数据。
        """
        key5 = f"baostock_5min_{code}"
        if all_5min is not None:
            cached = all_5min.get(key5)
        else:
            cached = self.cache.peek("5min", key5)
        lo_ts = pd.Timestamp(lo_ts)
        hi_ts = pd.Timestamp(hi_ts)
        hi_clip = self._hi_eff(hi_ts)

        def _clip(df5):
            # H6b：裁到请求窗口 [lo, hi] 闭区间（hi 为纯日期时含当日 15:00）
            return df5.loc[(df5.index >= lo_ts) & (df5.index <= hi_clip)]

        covers = (cached is not None and not cached.empty
                  and cached.index.max() >= hi_ts
                  and cached.index.min() <= lo_ts)
        if covers:
            # 本地 5min 缓存连续覆盖整个请求区间 → 直接插值返回（不落盘，C2）。
            return interpolate_5min_to_1min(_clip(cached))
        if self._offline:
            # 离线：5min 未完整覆盖且不联网，退化为已缓存段的插值（缺失段留空）
            if cached is not None and not cached.empty:
                return interpolate_5min_to_1min(_clip(cached))
            return None
        # C1 绝对约束：本地 5min 未覆盖请求区间时，回源 baostock 补齐缺失段
        # （之前为躲"每只卡 2.7s"而跳过回源，导致早期/缺口数据缺失，违反约束）。
        # 回源结果合并写回本地 5min 缓存（真实数据可落盘，C1）；插值出的 1m 仍不落盘（C2）。
        try:
            bs = self.sources.get("baostock")
            if bs is None:
                raise DataSourceError("baostock 源不可用")
            # 回源整个请求区间（baostock 单次查询代价固定，合并写库后下次命中本地）
            fresh = bs.get_5min(code, lo_ts.strftime("%Y-%m-%d"),
                                 hi_ts.strftime("%Y-%m-%d"))
            if fresh is not None and not fresh.empty:
                if cached is not None and not cached.empty:
                    merged = pd.concat([cached, fresh]).sort_index()
                    merged = merged[~merged.index.duplicated(keep="last")]
                else:
                    merged = fresh
                try:
                    self.cache.put("5min", key5, merged)
                except Exception as e:
                    import logging
                    logging.warning("[DataManager] baostock 5min 落盘失败 %s: %s", code, e)
                return interpolate_5min_to_1min(_clip(merged))
        except Exception as e:
            import logging
            logging.warning("[DataManager] baostock 回源 5min 失败 %s: %s", code, e)
        # 回源失败且本地有截断段 → 退化为截断插值（仍尽量给数据，而非返回 None）
        if cached is not None and not cached.empty:
            return interpolate_5min_to_1min(_clip(cached))
        return None

    def _load_real_minute(self, code, lo_ts, hi_ts, all_min):
        """缺口感知的真实分钟线获取（本地基底 + mootdx 仅补后续缺口）。

        本地命中 → 作为基底返回；仅当请求超出本地末端时才对“本地之后的缺口”
        回源 mootdx 并合并写回。这样不会用 mootdx 全量窗口覆盖掉本地较早的真实
        1 分钟——否则缓存会随“今天”推移不断缩水，早期数据被迫退化为插值。

        请求整体早于本地最早日期（且本地无数据）→ mootdx 也取不到 → 返回 None，
        交 baostock 5 分钟插值兜底。
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
                if self._offline:
                    pass  # 离线：不联网补缺口
                else:
                    try:
                        fresh = self.sources["mootdx"].get_minute(code)
                    except Exception as e:
                        import logging
                        logging.warning("[DataManager] mootdx回源补充缺口失败 %s: %s", code, e)
                        fresh = None
                if fresh is not None and not fresh.empty:
                    fresh = fresh[fresh.index > local_end]
                    if not fresh.empty:
                        combined = pd.concat([local, fresh]).sort_index()
                        combined = combined[~combined.index.duplicated(keep="last")]
                        # C1 绝对约束：本地缺失段由 mootdx 真实获取后必须落盘，
                        # 下次回测直接命中本地，避免反复联网。仅真实 1m 落盘，
                        # 插值(5m)数据绝不写库（见 _load_baostock_minute / C2）。
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
        if self._offline:
            # 离线：完全缺失的标的不再联网回源，直接返回 None 由上层跳过
            return None
        try:
            df = self.sources["mootdx"].get_minute(code)
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
                    # 原硬编码 ("mootdx","astock","tushare") 与 preload 的
                    # (tushare,mootdx,astock) 矛盾——同一代码价格帧来自
                    # tushare(前复权) 而 money 帧可能来自 mootdx(不复权)
                    for src in self._priority():
                        cached = self.cache.peek("daily", f"{src}_{code}")
                        if cached is not None and not (hasattr(cached, "empty") and cached.empty):
                            # 与 preload 同口径：tushare amount(千元)/vol(手)
                            # 归一为 money(元)/volume(股)
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
                    ddf["trade_dt"] = pd.to_datetime(
                        ddf["trade_date"].astype(str)).dt.date
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
        return TushareSource(token=token).test_connection()

    def list_sources(self):
        return [{"name": n, "priority": i, "available": True}
                for i, n in enumerate(self._priority())]
