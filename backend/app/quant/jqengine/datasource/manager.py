"""数据源优先级调度 + 自动降级 + 缓存管理器。"""

import os

import pandas as pd
from dotenv import set_key

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.tushare_src import TushareSource
from app.quant.jqengine.datasource.mootdx_src import MootdxSource
from app.quant.jqengine.datasource.astock_src import AStockSource
from app.quant.jqengine.datasource.baostock_src import BaostockSource, interpolate_5min_to_1min
from app.quant.jqengine.datasource.minute_synth import SyntheticMinuteSource
from app.quant.jqengine.datasource.base import DataSourceError
from app.quant.jqengine.config import CONFIG, REPO_ROOT

SOURCES = {"tushare": TushareSource, "mootdx": MootdxSource, "astock": AStockSource}


class DataManager:
    """按 ``DATASOURCE_PRIORITY`` 顺序尝试各数据源，单源失败自动降级到下一级。

    ``get_daily`` / ``get_minute`` 走本地缓存（按 ``{源}_{代码}`` 分键），
    重复读取不重复请求接口。
    """

    def __init__(self, token=None, cache=None):
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
        self._minute_mem = {}
        self._daily_mem = {}
        # 分钟线滑窗：回测中不持有整个回测区间的分钟数据，只保留最近 N 天，
        # 既省内存又避免每次按需加载都展开整段历史（见 get_minute / _ensure_minute_windowed）
        self.minute_lookback = pd.Timedelta(days=15)
        self._minute_cov = {}  # code -> (lo_ts, hi_ts) 已覆盖区间
        self._minute_real_cov = {}  # code -> (min_ts, max_ts) mootdx 真实分钟覆盖区间

    def set_minute_window(self, start, end):
        """设定分钟线展开区间（回测前调用），避免生成超长历史。

        注意：只重置分钟线缓存(_minute_mem)，不清空日线缓存(_daily_mem)。
        日线数据与分钟窗口无关；若在此清空 _daily_mem，会使 preload_daily()
        预加载的 1628 只ETF日线失效，导致回测首日 get_price 批量日线查询
        返回空、策略退化为防御模式(511880)，与聚宽参考产生系统性偏离。
        """
        self.minute_source.window = (pd.Timestamp(start), pd.Timestamp(end))
        self._minute_mem = {}

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
                        self._minute_cov[code] = (win[0], win[1])
                    count += 1
            except Exception:
                continue
        print(f"[preload] 分钟线: {count}/{len(codes)} 只")

    def preload_daily(self):
        """一次性加载全部日线缓存到内存，避免回测中逐文件读取。

        一条查询把整库日线缓存读入内存，按数据源优先级为每只标的选最优缓存，
        存入 _daily_mem。
        """
        all_daily = self.cache.get_all("daily")
        if not all_daily:
            print("[preload] 日线缓存为空")
            return
        priority = self._priority()
        # 按优先级为每只 ETF 选最优缓存
        best = {}  # jq_code -> (pri_idx, key)
        for key, df in all_daily.items():
            if "_" not in key:
                continue
            src_name, raw_code = key.split("_", 1)
            jq_code = self._to_jq_code(raw_code)
            pri = priority.index(src_name) if src_name in priority else len(priority)
            if jq_code not in best or pri < best[jq_code][0]:
                best[jq_code] = (pri, key)
        count = 0
        for jq_code, (_, key) in best.items():
            df = all_daily[key]
            if df is not None and not df.empty:
                self._daily_mem[f"get_daily_{jq_code}"] = df
                count += 1
        print(f"[preload] 日线: {count} 只ETF, {len(best)} 缓存键")

    def _priority(self):
        return [s for s in CONFIG["DATASOURCE_PRIORITY"] if s in self.sources]

    def fetch(self, method, *args, **kwargs):
        if method == "get_minute":
            code = args[0]
            end_date = args[1] if len(args) > 1 else None
            start_date = args[2] if len(args) > 2 else None
            return self.get_minute(code, end_date, start_date)
        cache_key = f"{method}_{args[0] if args else ''}"
        if cache_key in self._daily_mem:
            return self._daily_mem[cache_key]
        last_err = ""
        for name in self._priority():
            src = self.sources[name]
            try:
                if method in ("get_daily",):
                    code = args[0]
                    df = self.cache.get(
                        "daily", f"{name}_{code}",
                        lambda s=src: getattr(s, method)(*args, **kwargs),
                    )
                    if df is None or (hasattr(df, "empty") and df.empty):
                        raise DataSourceError(f"{name} 空数据")
                    self._daily_mem[cache_key] = df
                    return df
                result = getattr(src, method)(*args, **kwargs)
                self._daily_mem[cache_key] = result
                return result
            except Exception as e:
                last_err = f"{name}: {e}"
                continue
        raise DataSourceError(f"所有数据源失败: {last_err}")

    def get_minute(self, code, end_date=None, start_date=None):
        """返回分钟线（真实优先 + 合成补齐），按 end_date/start_date 切片。

        策略内取数走**滑窗**：只加载/保留截至 ``end_date`` 最近 ``minute_lookback``
        天的分钟数据，避免每次按需取数都展开整段历史（即"每次只读约一周"）。
        ``start_date`` 仅用于最终切片，不改变加载窗口。
        """
        as_of = end_date or start_date
        df = self._ensure_minute_windowed(code, as_of)
        if df is None or (hasattr(df, "empty") and df.empty):
            return pd.DataFrame()
        return self._slice_minute(df, end_date, start_date)

    def get_minute_feed(self, code, start, end):
        """桥接器时钟 feed 专用：加载并缓存**整段回测区间**的分钟线。

        与 :meth:`get_minute` 的滑窗不同，feed 需覆盖全部回测区间以驱动回放，
        因此按完整窗口加载（仅 1~2 只标的）。
        """
        df = self._minute_mem.get(code)
        if df is None:
            df = self._load_minute_merged(code, full=True)
            self._minute_mem[code] = df
            self._minute_cov[code] = (pd.Timestamp(start), pd.Timestamp(end))
        if df is None or (hasattr(df, "empty") and df.empty):
            return pd.DataFrame()
        return self._slice_minute(df, end, start)

    def _ensure_minute_windowed(self, code, as_of):
        """确保 ``_minute_mem[code]`` 已加载（首次按需加载后永久缓存）。

        回测中某标的第一次被取分钟数据时才合成/加载，之后永久复用，避免在
        热路径里反复重建。``minute_lookback`` 仅用于 ``full=False`` 的滑窗加载
        （长周期回测可借此只展开最近一段），短周期回测下全量加载与滑窗等价，
        故此处默认全量加载一次即可，代价与回测区间长度成正比而非与时间推进次数
        成正比。
        """
        df = self._minute_mem.get(code)
        if df is not None and not (hasattr(df, "empty") and df.empty):
            return df
        as_of_ts = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
        df = self._load_minute_merged(code, as_of=as_of_ts, full=True)
        if df is not None and not df.empty:
            self._minute_mem[code] = df
            win = self.minute_source.window
            if win:
                self._minute_cov[code] = (win[0], win[1])
        return df

    def get_minute_price_at(self, code, dt):
        """取某标的截至 ``dt`` 的最后一分钟收盘价（供实时价/下单价，O(1) 切片）。

        回测中 ``_live_price``/``get_current_data`` 每个 bar 每个持仓都会调用，
        直接对滑窗内的 ``_minute_mem`` 切片取末值；数据不在窗内则按需按滑窗加载。
        返回 ``float``；无数据返回 ``None``。
        """
        dt_ts = pd.Timestamp(dt)
        df = self._minute_mem.get(code)
        cov = self._minute_cov.get(code)
        if df is None or (hasattr(df, "empty") and df.empty) or cov is None or dt_ts > cov[1]:
            df = self._ensure_minute_windowed(code, dt_ts)
        if df is None or (hasattr(df, "empty") and df.empty):
            return None
        try:
            sub = df[df.index <= dt_ts]
            if sub.empty:
                return None
            return float(sub["close"].iloc[-1])
        except Exception:
            return None

    def _load_minute_merged(self, code, all_min=None, all_5min=None,
                            as_of=None, full=False):
        """三源合并（缺口感知）：本地基底 + mootdx 补后续缺口 + baostock 补前序缺口。

        - 本地 ``minute.db`` 里的真实 1 分钟：查到多少用多少（基底）；
        - 本地数据**之后**的缺口 → 回源 mootdx 获取（见 ``_load_real_minute``）；
        - 本地数据**之前**的缺口 → baostock 5 分钟插值成 1 分钟补齐；
        - 5 分钟线与 1 分钟线在缺口边界重叠的时间点：以 5 分钟线为准；
        - 三源皆无 → 日线合成兜底（仅兜底，确定性）。

        ``all_min`` / ``all_5min`` 可选，传入 ``cache.get_all`` 的结果以跳过逐键查询。
        ``as_of``/``full``：``full`` 展开整段回测区间（feed 用）；否则只展开截至
        ``as_of`` 最近 ``minute_lookback`` 天（滑窗，默认）。
        """
        if self.minute_source.window:
            win_start0 = self.minute_source.window[0]
            win_end0 = self.minute_source.window[1]
        else:
            win_start0 = pd.Timestamp.today() - pd.Timedelta(days=400)
            win_end0 = pd.Timestamp.today()
        if full:
            lo_ts, hi_ts = win_start0, win_end0
        else:
            hi_ts = pd.Timestamp(as_of) if as_of is not None else win_end0
            lo_ts = hi_ts - self.minute_lookback
            if lo_ts < win_start0:
                lo_ts = win_start0
        use_real = getattr(self, "_use_real_minute", True)

        layers = []
        real_start = None
        if use_real:
            real = self._load_real_minute(code, lo_ts, hi_ts, all_min)
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
            except Exception:
                pass

        if not layers:
            # 无真实分钟数据：回退到日线确定性合成（仅作兜底）
            synth = self.minute_source.get_minute(code, win_end0, win_start0)
            if synth is None or synth.empty:
                return pd.DataFrame()
            layers.append(synth)

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
        return merged.sort_index()

    def _load_baostock_minute(self, code, lo_ts, hi_ts, all_5min):
        """baostock 5 分钟线的本地优先获取 + 运行时插值。

        本地 ``5min.db`` 命中且覆盖请求区间 → 直接用；否则从 baostock 回源
        （仅拉取缺失区间），与已缓存区间合并后落盘 ``5min.db``；插值出的 1 分钟
        只在本进程内使用、**不落盘**（避免数据库膨胀）。

        注意: 缓存按 code 单键存储，若只缓存过局部区间(如某次只取了 4 月)，
        后续请求早期区间时必须重新回源并合并，否则会拿错区间的数据，导致
        早期 1 分钟缺口无法被 baostock 补齐。
        """
        key5 = f"baostock_5min_{code}"
        if all_5min is not None:
            cached = all_5min.get(key5)
        else:
            cached = self.cache.peek("5min", key5)
        covers = (cached is not None and not cached.empty
                  and cached.index.min() <= lo_ts
                  and cached.index.max() >= hi_ts)
        if covers:
            df5 = cached
        else:
            try:
                fresh = self.sources["baostock"].get_5min(
                    code,
                    lo_ts.strftime("%Y-%m-%d"),
                    hi_ts.strftime("%Y-%m-%d"),
                )
            except Exception:
                fresh = None
            if fresh is not None and not fresh.empty:
                if cached is not None and not cached.empty:
                    fresh = fresh.combine_first(cached)
                self.cache.put("5min", key5, fresh)
                df5 = fresh
            else:
                df5 = cached
        if df5 is None or df5.empty:
            return None
        # 13:10 / 14:55 等恰为 5 分钟整点，插值在边界处等于 5 分钟收盘，结果精确；
        # 其余时刻线性插值。插值结果仅运行时使用，不写库。
        return interpolate_5min_to_1min(df5)

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
                try:
                    fresh = self.sources["mootdx"].get_minute(code)
                except Exception:
                    fresh = None
                if fresh is not None and not fresh.empty:
                    fresh = fresh[fresh.index > local_end]
                    if not fresh.empty:
                        combined = pd.concat([local, fresh]).sort_index()
                        combined = combined[~combined.index.duplicated(keep="last")]
                        self.cache.put("minute", real_key, combined)
                        self._minute_real_cov[code] = (
                            combined.index.min(), combined.index.max())
                        return combined
            return local
        cov = self._minute_real_cov.get(code)
        if cov is not None and hi_ts < cov[0]:
            # 请求区间整体早于 mootdx 能提供的最早日期，回源也取不到 → 跳过
            return None
        try:
            df = self.sources["mootdx"].get_minute(code)
        except Exception:
            return None
        if df is not None and not df.empty:
            self.cache.put("minute", real_key, df)
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
