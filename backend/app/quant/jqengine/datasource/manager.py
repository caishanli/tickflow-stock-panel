"""数据源优先级调度 + 自动降级 + 缓存管理器。"""

import logging
import os

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
        self._money_memo = {}  # (codes_tuple, count) -> 全量成交额明细 DataFrame
        # 分钟线滑窗：回测中不持有整个回测区间的分钟数据，只保留最近 N 天，
        # 既省内存又避免每次按需加载都展开整段历史（见 get_minute / _ensure_minute_windowed）
        self.minute_lookback = pd.Timedelta(days=15)
        self._minute_cov = {}  # code -> (lo_ts, hi_ts) 已覆盖区间
        self._minute_real_cov = {}  # code -> (min_ts, max_ts) mootdx 真实分钟覆盖区间
        # 数据源取数失败计数：同一源连续返回空/异常 N 次即自动降级到末位并持久化，
        # 避免 tushare 对 ETF 永远返回空却每次都先试（慢且无意义）。
        self._src_fail = {}
        self._demote_threshold = 3
        self._demoted = set()

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
                # 预计算 trade_dt 列（date 对象），避免 get_daily_money_cached
                # 每天对每只重复 pd.to_datetime（全市场 1600+ 只 × 130 天极慢）。
                if "trade_dt" not in df.columns and "trade_date" in df.columns:
                    df = df.copy()
                    df["trade_dt"] = pd.to_datetime(
                        df["trade_date"].astype(str)).dt.date
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
        # C3：daily 本地优先。_daily_mem 命中即返回；未命中或覆盖不足时，
        # cache.get 会调 loader 回源（mootdx/tushare）并落盘。回测中
        # preload_daily 已预加载全量日线，此处一般命中内存。
        if cache_key in self._daily_mem:
            mem = self._daily_mem[cache_key]
            if mem is not None and not (hasattr(mem, "empty") and mem.empty):
                return mem
            del self._daily_mem[cache_key]
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
                    self._daily_mem[cache_key] = df
                    self._src_fail[name] = 0  # 该源成功取数，重置连续失败计数
                    return df
                result = getattr(src, method)(*args, **kwargs)
                self._daily_mem[cache_key] = result
                self._src_fail[name] = 0
                return result
            except Exception as e:
                last_err = f"{name}: {e}"
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
            raise RuntimeError(
                f"[DataManager] 分钟数据为空: {code} 在 {as_of} 时无可用分钟数据"
            )
        return self._slice_minute(df, end_date, start_date)

    def get_minute_feed(self, code, start, end):
        """桥接器时钟 feed 专用：加载并缓存**整段回测区间**的分钟线。

        与 :meth:`get_minute` 的滑窗不同，feed 需覆盖全部回测区间以驱动回放，
        因此按完整窗口加载（仅 1~2 只标的）。窗口上界用回测 end（而非 today），
        避免 full=True 用 today 超出 5min.db 覆盖而被迫回源 baostock 网络。
        """
        df = self._minute_mem.get(code)
        if df is None:
            # 加载整段回测区间 [start, end] 的分钟线（feed 仅 1~2 只标的）。
            # 窗口上界用 end（≤5min.db 覆盖），避免 full=True 用 today 超界回源。
            lo_ts = pd.Timestamp(start)
            hi_ts = pd.Timestamp(end)
            bs = self._load_baostock_minute(code, lo_ts, hi_ts, None)
            df = bs
            self._minute_mem[code] = df
            self._minute_cov[code] = (lo_ts, hi_ts)
        if df is None or (hasattr(df, "empty") and df.empty):
            raise RuntimeError(
                f"[DataManager] 分钟数据为空(feed): {code} 在 {start}~{end} 区间无可用分钟数据"
            )
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
        df = self._load_minute_merged(code, as_of=as_of_ts, full=False)
        if df is not None and not df.empty:
            self._minute_mem[code] = df
            win = self.minute_source.window
            if win:
                self._minute_cov[code] = (win[0], win[1])
        return df

    def preload_minute_for_pool(self, codes, as_of=None):
        """批量预热分钟线缓存：一次性加载 codes 中所有标的的全区间分钟数据到 _minute_mem。

        在策略构建好合并池后调用（如 `midday_routine` 后），使后续
        `get_price(..., frequency='1m')` 直接命中内存缓存，避免热路径联网/磁盘 IO。
        """
        as_of_ts = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
        logger.info("[DataManager] 开始预热分钟线缓存，标的数=%d", len(codes))
        for code in codes:
            if code in self._minute_mem:
                continue
            try:
                # 滑窗加载（最近 minute_lookback 天），避免 full=True 触发全周期窗口
                # 超出 5min.db 覆盖范围而被迫回源 baostock 网络。
                df = self._ensure_minute_windowed(code, as_of_ts)
                if df is not None and not (hasattr(df, "empty") and df.empty):
                    self._minute_mem[code] = df
                    win = self.minute_source.window
                    if win:
                        self._minute_cov[code] = (win[0], win[1])
            except Exception as e:
                logger.warning("预热分钟线失败 %s: %s", code, e)
        logger.info("[DataManager] 分钟线预热完成，已缓存 %d 只", len(self._minute_mem))

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
            except Exception as e:
                import logging
                logging.warning("[DataManager] baostock分钟数据获取失败 %s: %s", code, e)

        if not layers:
            # 三源皆无（mootdx/baostock 均失败）→ 最后兜底：日线合成分钟。
            # 仅内存使用、绝不落盘（C2：非真实源数据不写库）。确定性、可复现。
            try:
                synth = self.minute_source.get_minute(code, hi_ts, lo_ts)
                if synth is not None and not synth.empty:
                    return synth
            except Exception as e:
                import logging
                logging.warning("[DataManager] 日线合成兜底失败 %s: %s", code, e)
            raise RuntimeError(
                f"[DataManager] 分钟数据获取失败: {code} 在 {lo_ts.date()}~{hi_ts.date()} "
                f"区间内 mootdx/baostock 均失败且日线合成兜底也失败。"
            )

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
                  and cached.index.max() >= hi_ts
                  and cached.index.min() <= lo_ts)
        if covers:
            # 5min.db 连续覆盖整个请求区间 → 直接插值返回（不落盘，C2）。
            return interpolate_5min_to_1min(cached)
        # C1 绝对约束：本地 5min 未覆盖请求区间时，回源 baostock 补齐缺失段
        # （之前为躲"每只卡 2.7s"而跳过回源，导致早期/缺口数据缺失，违反约束）。
        # 回源结果合并写回 5min.db（真实数据可落盘，C1）；插值出的 1m 仍不落盘（C2）。
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
                return interpolate_5min_to_1min(merged)
        except Exception as e:
            import logging
            logging.warning("[DataManager] baostock 回源 5min 失败 %s: %s", code, e)
        # 回源失败且本地有截断段 → 退化为截断插值（仍尽量给数据，而非返回 None）
        if cached is not None and not cached.empty:
            df5 = cached
            if cached.index.min() > lo_ts:
                df5 = cached.loc[cached.index >= lo_ts]
            return interpolate_5min_to_1min(df5)
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
            # 请求区间整体早于 mootdx 能提供的最早日期，回源也取不到 -> 跳过
            return None
        # C1.b：mootdx 仅覆盖近 ~3 个月。请求末尾早于此范围 -> mootdx 取不到，
        # 跳过交 baostock 5min 插值（C1.b）。
        mootdx_floor = pd.Timestamp.now() - pd.Timedelta(days=95)
        if hi_ts < mootdx_floor:
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

    def preload_minute_for_pool(self, codes, as_of):
        """批量预加载分钟线到内存缓存（供策略午盘动量计算使用）。

        直接调用 ``_ensure_minute_windowed`` 对每个代码全量加载，填充
        ``_minute_mem`` 字典。后续 ``get_price(frequency='1m')`` 即可命中缓存。
        """
        as_of_ts = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
        loaded = 0
        for code in codes:
            try:
                # 滑窗加载（最近 minute_lookback 天），避免 full=True 触发全周期窗口
                # 超出 5min.db 覆盖范围而被迫回源 baostock 网络。
                df = self._ensure_minute_windowed(code, as_of_ts)
                if df is not None and not df.empty:
                    loaded += 1
            except Exception as e:
                logger.warning("预加载分钟线失败 %s: %s", code, e)
        logger.info("分钟线池预加载完成: 成功 %d/%d", loaded, len(codes))

    def get_daily_money_cached(self, codes, end_date, count=3):
        """从缓存直接取日线成交额，避免走 get_price 链路。

        返回 DataFrame(columns=['code','time','money'])，仅包含有数据的行。

        性能：历史日线不变，对固定 ``codes``+``count`` 的全量明细只算一次后
        memo 化；后续每日调用仅按 ``end_date`` 切片（O(行数)），避免回测中
        每天对全市场 1600+ 只重复 to_datetime/过滤（原 ~1.1s/天 → 累计数分钟）。
        """
        memo_key = (tuple(sorted(codes)), count)
        full = self._money_memo.get(memo_key)
        if full is None:
            full = self._build_money_full(codes, count)
            self._money_memo[memo_key] = full
        if full is None or full.empty:
            return pd.DataFrame(columns=["code", "time", "money"])
        end_dt = pd.Timestamp(end_date).date()
        return full[full["time"].dt.date <= end_dt]

    def _build_money_full(self, codes, count=3):
        """构建全量成交额明细（不按 end_date 过滤），结果 memo 化复用。

        列：``code`` / ``time``(Timestamp) / ``money``。仅含成交额>0 的行。
        """
        out_rows = []
        for code in codes:
            try:
                # C3：daily 必须真实，本地优先，本地全源缺失即回源并落盘。
                ddf = self._daily_mem.get("get_daily_" + code)
                if ddf is None or (hasattr(ddf, "empty") and ddf.empty):
                    for src in ("mootdx", "astock", "tushare"):
                        cached = self.cache.peek("daily", f"{src}_{code}")
                        if cached is not None and not (hasattr(cached, "empty") and cached.empty):
                            ddf = cached
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
                trade_dt = ddf["trade_dt"]
                sub = ddf.tail(count)
                if sub.empty:
                    continue
                money_col = sub["money"] if "money" in sub.columns else sub.get("amount")
                amt_col = sub["amount"] if "amount" in sub.columns else None
                if money_col is None and amt_col is not None:
                    money_col = amt_col
                elif money_col is not None and amt_col is not None:
                    money_col = money_col.fillna(amt_col)
                if money_col is None:
                    continue
                mvals = money_col.astype(float).to_numpy()
                valid_mask = mvals > 0
                if not valid_mask.any():
                    continue
                sub_idx = sub.index.to_numpy()[valid_mask]
                sub_m = mvals[valid_mask]
                for ridx, mv in zip(sub_idx, sub_m):
                    out_rows.append({
                        "code": code,
                        "time": pd.Timestamp(trade_dt.loc[ridx]),
                        "money": float(mv)
                    })
            except Exception:
                continue
        if not out_rows:
            return pd.DataFrame(columns=["code", "time", "money"])
        return pd.DataFrame(out_rows)

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
