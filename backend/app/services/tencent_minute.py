"""腾讯分钟 K 线回源（mootdx 熔断期的备用史源）。

背景：mootdx K 线可能全局返回空（如 2026-09-10 起全市场 bars 空响应，
疑似出口 IP 被限），此时分钟/日线批量回源整批失败。腾讯 ``mkline``
（``ifzq.gtimg.cn``）零鉴权、HTTP、可做近期分钟回补。

接口实测（2026-09-11/12）：
- ``.../mkline?param={sh|sz}{code},m1,,800``：最近 ~800 根 1m（约 3.3 个
  交易日；320 根只够 1.3 日——09-10 熔断案例 9.11 收盘回补时 9.10 只剩
  尾段 13:42~15:00 共 79 根，整段上午缺失且此后无人能补；800 根可覆盖
  T-2/T-3 缺口）；每交易日 241 根（09:30~11:30 + 13:01~15:00，无 13:00
  bar，与 mootdx 口径一致）；
- 行格式 ``[时间, 开, 收, 高, 低, 量(手), {}, 换手率(基点)]``——注意第 2
  列是**收**不是高（已用 600000 全天 rollup vs 快照 OHLCV 逐项对齐验证，
  量合计 653274 vs 653273 手）；
- 第 7 列是换手率基点**不是成交额**；成交额无字段，按
  ``volume(股) × typical((H+L+C)/3)`` 估算（分钟级回测/展示够用，日线级
  对账以 TickFlow 日线为准）；
- ETF 同接口（sh510300 等）可用；北交所跳过（与 mootdx 口径一致）。

限流：腾讯 5000+ 连击会返回空（限流非封禁，降速可恢复）——本模块默认
4 worker + 空响应退避（2/5/10s）+ 全局连续空 ≥30 时休眠 60s。

触发纪律：仅当近几日有已收盘分钟缺口 **且** K 线熔断开路时由调度侧
调用（见 ``fill_recent_gaps``），mootdx 部分成功/健康时不混源。落盘复用
mootdx 分区写入（读旧→concat→unique keep-last→原子替换），mootdx 恢复后
的全量写自然 supersede。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date as _date
from datetime import datetime as _datetime

import polars as pl
import requests

logger = logging.getLogger(__name__)

_MKLINE_URL = "https://ifzq.gtimg.cn/appstock/app/kline/mkline?param="
_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Referer": "https://gu.qq.com/"}
_HTTP_TIMEOUT = 10.0
_MAX_RETRIES = 3
_BACKOFF = (2.0, 5.0, 10.0)
_THROTTLE_STREAK = 30   # 全局连续空响应达此数 → 休眠 60s（腾讯限流，可恢复）
_THROTTLE_SLEEP = 60.0
# 单请求拉取根数：mkline 实测上限 800（>800 仍返回 800）。320≈1.3 个交易日
# 不够回补 T-2 缺口（09-10 案例：9.11 收盘回补时 9.10 只够到 10:49），
# 800≈3.3 个交易日。env 可调，上限 800。
_BARS_CAP = 800


def _bars() -> int:
    try:
        return min(_BARS_CAP, max(1, int(os.getenv("TENCENT_MINUTE_BARS", "") or _BARS_CAP)))
    except ValueError:
        return _BARS_CAP

_throttle_lock = threading.Lock()
_throttle_streak = 0


def _workers() -> int:
    try:
        return max(1, int(os.getenv("TENCENT_MINUTE_WORKERS", "") or 4))
    except ValueError:
        return 4


def _pacing() -> float:
    """单请求间隔秒数（防 WAF 限流；2026-09-11 全市场 ~2 万请求曾触发
    腾讯/新浪 501/拒绝访问）。"""
    try:
        return max(0.0, float(os.getenv("TENCENT_MINUTE_PACING_S", "") or 0.05))
    except ValueError:
        return 0.05


def tf_to_vendor(symbol: str) -> str | None:
    """分区 symbol (.SH/.SZ) -> 腾讯 vendor 码 (sh/sz+6位)。北交所返回 None。"""
    pure, _, mkt = symbol.partition(".")
    if len(pure) != 6 or not pure.isdigit():
        return None
    if mkt == "SH":
        return f"sh{pure}"
    if mkt == "SZ":
        return f"sz{pure}"
    return None


def _note_empty() -> None:
    """记录一次空响应；达限流阈值时休眠（调用方已在锁外，不嵌套取锁）。"""
    global _throttle_streak
    with _throttle_lock:
        _throttle_streak += 1
        streak = _throttle_streak
    if streak >= _THROTTLE_STREAK:
        logger.warning("腾讯分钟回源连续空 %d 次（疑似限流），休眠 %ds",
                       streak, int(_THROTTLE_SLEEP))
        time.sleep(_THROTTLE_SLEEP)
        with _throttle_lock:
            _throttle_streak = 0


def _note_ok() -> None:
    global _throttle_streak
    with _throttle_lock:
        _throttle_streak = 0


def fetch_m1(session: requests.Session, vendor: str) -> list | None:
    """拉单标的 m1（~800 根）。None=网络/HTTP 失败；[] = 无数据（限流或停牌）。

    停牌股返回 ``{"code":0,...,"data":{vendor: {"qt":..., "m1":[]}}}`` 或缺 m1。
    """
    url = f"{_MKLINE_URL}{vendor},m1,,{_bars()}"
    try:
        resp = session.get(url, headers=_HEADERS, timeout=_HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    try:
        node = (payload.get("data") or {}).get(vendor) or {}
        rows = node.get("m1") or []
        return list(rows)
    except Exception:
        return None


def parse_m1_rows(symbol: str, rows: list, days: set[_date]) -> pl.DataFrame:
    """m1 行 -> 分钟分区帧（北京 naive；volume 股；amount 估算）。

    只保留 ``days`` 内 bar；非法行丢弃。空输入返回空帧（不断言）。
    """
    out: list[tuple] = []
    for r in rows:
        try:
            if len(r) < 6:
                continue
            dt = _datetime.strptime(str(r[0]), "%Y%m%d%H%M")
            if dt.date() not in days:
                continue
            o, c, h, lo = float(r[1]), float(r[2]), float(r[3]), float(r[4])
            if o <= 0 or c <= 0 or h <= 0 or lo <= 0:
                continue
            vol = float(r[5]) * 100.0
            if vol < 0:
                continue
            typical = (h + lo + c) / 3.0
            out.append((symbol, dt, o, h, lo, c, vol, vol * typical))
        except (TypeError, ValueError, IndexError, KeyError, AttributeError):
            continue
    if not out:
        return pl.DataFrame(schema={
            "symbol": pl.String, "datetime": pl.Datetime("us"),
            "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
            "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
        })
    cols = list(zip(*out, strict=True))
    return pl.DataFrame({
        "symbol": list(cols[0]), "datetime": list(cols[1]),
        "open": list(cols[2]), "high": list(cols[3]), "low": list(cols[4]),
        "close": list(cols[5]), "volume": list(cols[6]),
        "amount": list(cols[7]),
    }, schema_overrides={"datetime": pl.Datetime("us")})


def _fetch_symbol(session: requests.Session, symbol: str,
                  days: set[_date]) -> pl.DataFrame | None:
    """单标的抓取+解析（带重试/退避）。None=彻底失败；空帧=无数据（停牌等）。"""
    vendor = tf_to_vendor(symbol)
    if vendor is None:
        return pl.DataFrame()
    rows: list | None = None
    for attempt in range(_MAX_RETRIES):
        rows = fetch_m1(session, vendor)
        if rows:
            _note_ok()
            break
        _note_empty()
        if rows is not None:
            break  # 空响应多为 WAF 限流：不重试（全局计数达阈值统一休眠）
        if attempt < _MAX_RETRIES - 1:
            time.sleep(_BACKOFF[attempt])
    if not rows:
        return None if rows is None else pl.DataFrame()
    return parse_m1_rows(symbol, rows, days)


def backfill_days(symbols: list[str], days: list[_date],
                  progress: str = "腾讯分钟回源") -> dict:
    """多标的 m1 回补。返回 {"frames": {day: [pl.DataFrame]},
    "ok_symbols": [...], "uncovered": [...]}。

    ``uncovered``：网络失败/全空/解析后无目标日 bar 的标的（含停牌）。
    """
    day_set = set(days)
    frames_by_day: dict[_date, list[pl.DataFrame]] = {d: [] for d in days}
    ok_symbols: list[str] = []
    uncovered: list[str] = []
    local = threading.local()

    def _session() -> requests.Session:
        s = getattr(local, "s", None)
        if s is None:
            s = requests.Session()
            local.s = s
        return s

    def _one(sym: str):
        try:
            out = _fetch_symbol(_session(), sym, day_set)
            if _pacing():
                time.sleep(_pacing())
            return sym, out
        except Exception as e:
            logger.debug("腾讯分钟 %s 异常: %s", sym, e)
            return sym, None

    total = len(symbols)
    done = {"n": 0}
    with ThreadPoolExecutor(max_workers=_workers(),
                            thread_name_prefix="tencent-min") as ex:
        futures = [ex.submit(_one, s) for s in symbols]
        for fut in futures:
            sym, frame = fut.result()
            done["n"] += 1
            if done["n"] % 500 == 0 or done["n"] == total:
                logger.info("%s 进度 %d/%d", progress, done["n"], total)
            if frame is None or frame.is_empty():
                uncovered.append(sym)
                continue
            ok_symbols.append(sym)
            for d in days:
                sub = frame.filter(pl.col("datetime").dt.date() == d)
                if not sub.is_empty():
                    frames_by_day[d].append(sub)
    return {"frames": frames_by_day, "ok_symbols": ok_symbols,
            "uncovered": uncovered}


def _wanted_days(candidates: list[_date]) -> list[_date]:
    """过滤掉未来日（时钟漂移保护）。"""
    today = _dt.date.today()
    return [d for d in candidates if d <= today]


def _partition_keys(root, day: _date) -> pl.DataFrame | None:
    """读某日分钟分区的 (symbol, datetime) 键集（缺分区/坏文件/空表返回 None）。

    用于缺口填充的两件事：① per-symbol bar 数判定「当日完整」（09-10 案例：
    分区含全部 symbol 但每只仅尾段 79 根——symbol 出现 ≠ 完整）；② 反连接
    过滤，只保留分区缺失的 (symbol, datetime) 行，任意位置的洞（上午/尾段/
    中段）都能补且绝不覆盖既有 bar。
    """
    part = root / f"date={day.isoformat()}" / "part.parquet"
    if not part.exists():
        return None
    try:
        df = pl.read_parquet(part, columns=["symbol", "datetime"])
    except Exception:
        return None
    return None if df.is_empty() else df


def _bar_counts(keys: pl.DataFrame) -> dict[str, int]:
    """(symbol, datetime) 键集 → 每 symbol bar 数。"""
    cnt = keys.group_by("symbol").len()
    return dict(zip(cnt["symbol"].to_list(), cnt["len"].to_list(), strict=True))


# 「当日完整」的 bar 数下限：正常完整日每 symbol 241 根（09:30~15:00），
# 停牌/一字板合法短缺 ≤~24 根（09-11 实测全市场最短 217），阈值 200 与
# mootdx 侧 _short_bar_minute_days 同口径。低于它的 symbol 每轮重拉（反连接
# 下多为 no-op），高于它的绝不重复回补。
_DAY_COMPLETE_MIN_BARS = max(1, int(os.getenv("TENCENT_MINUTE_DAY_MIN_BARS", "") or 200))


def _day_complete(counts: dict[str, int], sym: str) -> bool:
    return counts.get(sym, 0) >= _DAY_COMPLETE_MIN_BARS


def sync_stock_minute_tencent(days: list[_date], only_missing: bool = True) -> dict:
    """腾讯回补股票分钟到 ``kline_minute`` 分区（merge 写入）。

    复用 mootdx 的宇宙/上市过滤/落盘函数。``only_missing`` 为真时按「当日
    bar 数」判定已覆盖（≥ ``_DAY_COMPLETE_MIN_BARS``）：各目标日都完整的
    symbol 整批跳过（连网都不碰）。对未覆盖 symbol，按 (symbol, datetime)
    与分区键集**反连接**，只补缺失时间戳——洞在上午/尾段/中段都能填，
    且绝不覆盖任何已落盘 bar（09-10 案例即为「尾段已有、整段上午缺失」）。
    返回 {"rows": {day: n}, "total": int, "uncovered": [...],
    "source": "tencent"}。
    """
    from app.services import mootdx_service as ms
    days = _wanted_days(days)
    if not days:
        return {"rows": {}, "total": 0, "uncovered": [], "source": "tencent"}
    stocks = [s for s in ms._stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("tencent_minute: 股票宇宙为空，跳过")
        return {"rows": {}, "total": 0, "uncovered": [], "source": "tencent"}
    listing = ms._listing_date_map()
    pre_delisted = {s for s in stocks if listing.get(s) == _date(1970, 1, 1)}
    stocks = [s for s in stocks if s not in pre_delisted]
    # 上市晚于最早目标日的标的在目标日无数据，整批跳过（逐日过滤冗余）
    earliest = min(days)

    def _listed(sym: str) -> bool:
        ld = listing.get(sym)
        return ld is None or ld <= earliest

    stocks = [s for s in stocks if _listed(s)]
    per_day_keys: dict[_date, pl.DataFrame | None] = {}
    per_day_counts: dict[_date, dict[str, int]] = {}
    complete: set[str] = set()
    if only_missing:
        per_day_keys = {d: _partition_keys(ms.STOCK_MINUTE_ROOT, d) for d in days}
        per_day_counts = {d: (_bar_counts(k) if k is not None else {})
                          for d, k in per_day_keys.items()}
        complete = {s for s in stocks
                    if all(_day_complete(per_day_counts[d], s) for d in days)}
        skipped = [s for s in stocks if s in complete]
        stocks = [s for s in stocks if s not in complete]
        if skipped:
            logger.info("tencent_minute: 股票 %d 只各目标日均完整，跳过", len(skipped))
    if not stocks:
        return {"rows": {d.isoformat(): 0 for d in days}, "total": 0,
                "uncovered": [], "source": "tencent"}
    res = backfill_days(stocks, days, progress="腾讯股票分钟回源")
    rows: dict[str, int] = {}
    total = 0
    for d in days:
        frames = res["frames"][d]
        if only_missing:
            keys = per_day_keys[d]
            if keys is not None and frames:
                combined = pl.concat(frames).join(
                    keys, on=["symbol", "datetime"], how="anti")
                frames = [combined] if not combined.is_empty() else []
        if not frames:
            rows[d.isoformat()] = 0
            continue
        ms._flush_stock_minute_chunk(frames)
        n = sum(f.height for f in frames)
        rows[d.isoformat()] = n
        total += n
        logger.info("tencent_minute: 股票分钟 %s 补洞 %d 行", d.isoformat(), n)
    logger.warning("tencent_minute: 股票分钟回补完成 %d 行，未覆盖 %d 只",
                   total, len(res["uncovered"]))
    return {"rows": rows, "total": total, "uncovered": res["uncovered"],
            "source": "tencent"}


def sync_etf_minute_tencent(days: list[_date], only_missing: bool = True) -> dict:
    """腾讯回补 ETF 分钟到 ``kline_etf_minute`` 分区（merge 写入）。

    ``only_missing`` 语义同股票侧：按「当日 bar 数」判定已覆盖，未覆盖
    symbol 按 (symbol, datetime) 反连接只补缺失时间戳，不覆盖已有 bar。
    """
    from app.services import mootdx_service as ms
    days = _wanted_days(days)
    if not days:
        return {"rows": {}, "total": 0, "uncovered": [], "source": "tencent"}
    codes = ms._etf_universe()
    tf_syms = []
    for jq in codes:
        try:
            pure, mkt = jq.split(".")
            tf_syms.append(pure + (".SH" if mkt == "XSHG" else ".SZ"))
        except ValueError:
            continue
    if not tf_syms:
        logger.warning("tencent_minute: ETF 宇宙为空，跳过")
        return {"rows": {}, "total": 0, "uncovered": [], "source": "tencent"}
    per_day_keys: dict[_date, pl.DataFrame | None] = {}
    per_day_counts: dict[_date, dict[str, int]] = {}
    complete: set[str] = set()
    if only_missing:
        per_day_keys = {d: _partition_keys(ms.ETF_MINUTE_ROOT, d) for d in days}
        per_day_counts = {d: (_bar_counts(k) if k is not None else {})
                          for d, k in per_day_keys.items()}
        complete = {s for s in tf_syms
                    if all(_day_complete(per_day_counts[d], s) for d in days)}
        skipped = [s for s in tf_syms if s in complete]
        tf_syms = [s for s in tf_syms if s not in complete]
        if skipped:
            logger.info("tencent_minute: ETF %d 只各目标日均完整，跳过", len(skipped))
    if not tf_syms:
        return {"rows": {d.isoformat(): 0 for d in days}, "total": 0,
                "uncovered": [], "source": "tencent"}
    res = backfill_days(tf_syms, days, progress="腾讯ETF分钟回源")
    rows: dict[str, int] = {}
    total = 0
    for d in days:
        frames = res["frames"][d]
        if only_missing:
            keys = per_day_keys[d]
            if keys is not None and frames:
                combined = pl.concat(frames).join(
                    keys, on=["symbol", "datetime"], how="anti")
                frames = [combined] if not combined.is_empty() else []
        if not frames:
            rows[d.isoformat()] = 0
            continue
        out = pl.concat(frames).unique(
            subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
        n = ms._write_minute_partition(out, ms.ETF_MINUTE_ROOT, d)
        rows[d.isoformat()] = n
        total += n
    logger.warning("tencent_minute: ETF分钟回补完成 %d 行，未覆盖 %d 只",
                   total, len(res["uncovered"]))
    return {"rows": rows, "total": total, "uncovered": res["uncovered"],
            "source": "tencent"}


def fill_recent_gaps(kind: str = "stock", lookback: int = 5) -> dict | None:
    """近期分钟缺口的腾讯兜底（调度侧唯一入口；健康时零开销 no-op）。

    缺口两类（都限定近 ``lookback`` 个交易日窗口）：
    1. mootdx 分区缺失的已收盘交易日（盘中今天除外，见
       ``_missing_*_minute_days``——半程不落盘）；
    2. 分区存在但 **bar 数量级残缺** 的交易日（``_short_bar_minute_days``：
       symbol 覆盖齐但每只 bar 数远低于全天，如 09-10 熔断截断日——symbol
       覆盖率类检测对此盲，若不补检该类缺口会永久搁浅）。
    同时满足才回补：
    1. 窗口内有上述任一缺口；
    2. K 线熔断开路（mootdx 已确认不可用，而非偶发抖动；mootdx 健康时
       启动巡检会用自家数据 range 重写残缺日，见 mootdx_service 巡检段）。
    不触发返回 None（调用方只记 debug）；触发返回回补结果。
    """
    from app.quant.jqengine.datasource.mootdx_breaker import kline_allowed
    from app.services import mootdx_service as ms
    if kind not in ("stock", "etf"):
        raise ValueError(f"未知 kind: {kind}")
    if kind == "etf":
        missing = set(ms._missing_minute_days()[-lookback:])
        short = ms._short_bar_minute_days(ms.ETF_MINUTE_ROOT, lookback=lookback)
    else:
        missing = set(ms._missing_stock_minute_days()[-lookback:])
        short = ms._short_bar_minute_days(ms.STOCK_MINUTE_ROOT, lookback=lookback)
    days = sorted(missing | set(short))
    if not days:
        return None
    if kline_allowed():
        logger.debug("tencent_minute: %s分钟缺口 %s 但熔断关闭，mootdx 自行处理",
                     kind, [d.isoformat() for d in days])
        return None
    logger.warning("tencent_minute: %s分钟缺口 %s 且熔断开路，切腾讯回补",
                   kind, [d.isoformat() for d in days])
    if kind == "etf":
        return sync_etf_minute_tencent(days)
    return sync_stock_minute_tencent(days)
