"""日线备用史源链：腾讯 fqkline(day) 第一，新浪 K线(scale=240) 第二，mootdx 第三。

背景：mootdx K 线全局返回空 + TickFlow 免费日线 T+1/限流时，日线分区
（kline_daily / kline_etf_daily）缺口无人补。本模块只补**已收盘缺口日**，
纯缺口填充（only_missing），永不覆盖已有 bar。

口径实测（2026-09-11）：
- 腾讯 ``fqkline/get?param={sh|sz}{code},day,,,N,qfq``：只有 qfq 味（无 raw
  档）；行格式 ``[日期, 开, 收, 高, 低, 量(手)]``（第 2 列是**收**，已用
  600000 全天 rollup 对齐验证）；近 N 天窗口无除权时 qfq==raw；
- 新浪 ``getKLineData?symbol=&scale=240``：raw 日线，
  ``{day, open, high, low, close, volume(股)}``，含当日 bar；
- 量纲：股票分区 volume=手，ETF 分区 volume=股（与 mootdx 落盘口径一致）；
  amount 两源皆无，按 ``volume(股)×typical((H+L+C)/3)`` 估算；
- 除权说明：腾讯为 qfq 口径；回补后用 TickFlow raw 日线全量审计（09-10
  有基准），差异标的用 prefer="sina" + overwrite 覆盖重填（09-11 为最新
  日，qfq 恒等于 raw，无需审计）。

触发纪律同分钟 fallback（见 tencent_minute.fill_recent_gaps）：近 lookback
天有已收盘缺口 **且** K 线熔断开路才回补。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date as _date

import polars as pl
import requests

logger = logging.getLogger(__name__)

_TENCENT_DAY_URL = "https://ifzq.gtimg.cn/appstock/app/fqkline/get?param="
_SINA_KLINE_URL = "https://quotes.sina.cn/cn/api/openapi.php/CN_MarketDataService.getKLineData"
_TENCENT_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                    "Referer": "https://gu.qq.com/"}
_SINA_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                 "Referer": "https://finance.sina.com.cn"}
_HTTP_TIMEOUT = 15.0
_MAX_RETRIES = 3
_BACKOFF = (2.0, 5.0, 10.0)

#: 备用源请求失败明细（(源, 明细)）：``_get_json`` 追加，``backfill_days``
#: 起止重置/汇总打一条聚合 warning（2026-09-11 教训：腾讯 501/新浪 456
#: 被静默吞掉，全市场 0 行却零诊断）。跨线程共享故配锁；并发多轮
#: backfill_days 会混计（调度侧串行调用，无此问题）。
_FETCH_ERR_LOCK = threading.Lock()
_FETCH_ERRORS: list[tuple[str, str]] = []


def _note_fetch_error(source: str, detail: str) -> None:
    with _FETCH_ERR_LOCK:
        _FETCH_ERRORS.append((source, detail))


def _drain_fetch_errors() -> list[tuple[str, str]]:
    """取走并清空失败明细（backfill_days 起止各调一次，避免跨轮混计）。"""
    with _FETCH_ERR_LOCK:
        errs = _FETCH_ERRORS.copy()
        _FETCH_ERRORS.clear()
        return errs


_DAY_SCHEMA = {
    "symbol": pl.String, "date": pl.Date,
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
    "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
}


def _workers() -> int:
    try:
        return max(1, int(os.getenv("ALT_DAILY_WORKERS", "") or 4))
    except ValueError:
        return 4


def _pacing() -> float:
    """单请求间隔秒数（防 WAF 限流；2026-09-11 全市场曾触发腾讯 501 /
    新浪拒绝访问）。"""
    try:
        return max(0.0, float(os.getenv("ALT_DAILY_PACING_S", "") or 0.1))
    except ValueError:
        return 0.1


def tf_to_vendor(symbol: str) -> str | None:
    """分区 symbol (.SH/.SZ) -> 腾讯/新浪 vendor 码。北交所返回 None。"""
    pure, _, mkt = symbol.partition(".")
    if len(pure) != 6 or not pure.isdigit():
        return None
    if mkt == "SH":
        return f"sh{pure}"
    if mkt == "SZ":
        return f"sz{pure}"
    return None


def _get_json(session: requests.Session, url: str, headers: dict,
              source: str = "") -> dict | None:
    try:
        resp = session.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, dict) else None
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status is not None:
            _note_fetch_error(source or "http", f"HTTP {status}")
        else:
            _note_fetch_error(source or "http",
                              f"{type(e).__name__}:{str(e)[:60]}")
        return None


def fetch_tencent_day(session: requests.Session, vendor: str, count: int = 10) -> list | None:
    """腾讯前复权日线（近 count 根）。None=失败；[] = 无数据。"""
    payload = _get_json(
        session, f"{_TENCENT_DAY_URL}{vendor},day,,,{count},qfq", _TENCENT_HEADERS,
        source="tencent")
    try:
        node = ((payload or {}).get("data") or {}).get(vendor) or {}
        return list(node.get("qfqday") or [])
    except Exception:
        return None


def fetch_sina_day(session: requests.Session, vendor: str, count: int = 10) -> list | None:
    """新浪 raw 日线。None=失败；[] = 无数据。"""
    payload = _get_json(
        session,
        f"{_SINA_KLINE_URL}?symbol={vendor}&scale=240&ma=no&datalen={count}",
        _SINA_HEADERS, source="sina")
    try:
        return list(((payload or {}).get("result") or {}).get("data") or [])
    except Exception:
        return None


def _rows_cover(rows: list, name: str, days: set[_date]) -> bool:
    """某源返回是否已覆盖全部目标日（日期提取失败按未覆盖）。"""
    have: set[_date] = set()
    for r in rows:
        try:
            d = _parse_date(r[0] if name == "tencent" else r.get("day", ""))
            if d in days:
                have.add(d)
        except (TypeError, ValueError, IndexError, KeyError, AttributeError):
            continue
    return have >= set(days)


def _parse_date(s: str) -> _date | None:
    try:
        return _date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


def _row_frame(symbol: str, day: _date, o: float, h: float, lo: float,
               c: float, vol_shares: float, vol_divisor: float) -> dict | None:
    """单 bar 行：volume 按表口径折算（股票÷100=手，ETF/指数保持股），
    amount 恒按股×typical 估算。"""
    if o <= 0 or h <= 0 or lo <= 0 or c <= 0 or vol_shares <= 0:
        # vol==0 的"幽灵 bar"（停牌占位行）丢弃：无真实成交，不应落盘
        return None
    volume = vol_shares / vol_divisor
    return {"symbol": symbol, "date": day, "open": o, "high": h, "low": lo,
            "close": c, "volume": float(volume),
            "amount": float(vol_shares * (h + lo + c) / 3.0)}


def build_symbol_frame(symbol: str, vol_divisor: float, days: set[_date],
                       session: requests.Session,
                       prefer: str = "tencent") -> pl.DataFrame | None:
    """单标的按 prefer 源优先、另一源补缺的链取缺口日。

    None=两源皆网络失败；空帧=无缺口数据（停牌等）。除权说明：腾讯为 qfq
    口径，窗口内（目标日~今天）有除权时老 bar 与 raw 有差——回补后用
    TickFlow raw 日线全量审计 09-10（09-11 qfq 恒等于 raw），差异标的用
    prefer="sina" 覆盖重填。
    """
    vendor = tf_to_vendor(symbol)
    if vendor is None:
        return pl.DataFrame(schema=_DAY_SCHEMA)
    order = ("sina", "tencent") if prefer == "sina" else ("tencent", "sina")
    got: dict[str, list | None] = {"tencent": None, "sina": None}
    for name in order:
        fetch = fetch_sina_day if name == "sina" else fetch_tencent_day
        rows: list | None = None
        for attempt in range(_MAX_RETRIES):
            rows = fetch(session, vendor)
            if rows:
                break
            if rows is not None:
                break  # 空响应多为 WAF 限流：不重试
            if attempt < _MAX_RETRIES - 1:
                time.sleep(_BACKOFF[attempt])
        got[name] = rows
        if rows and _rows_cover(rows, name, days):
            break  # 首源已覆盖全部缺口日，次源免拉（省一半请求）
    t_rows, s_rows = got["tencent"], got["sina"]
    t_map: dict[_date, list] = {}
    for r in t_rows or []:
        try:
            if len(r) < 6:
                continue
            d = _parse_date(r[0])
            if d in days:
                t_map[d] = [float(r[1]), float(r[2]), float(r[3]),
                            float(r[4]), float(r[5]) * 100.0]
        except (TypeError, ValueError, IndexError, KeyError, AttributeError):
            continue
    s_map: dict[_date, dict] = {}
    if s_rows is not None:
        for r in s_rows:
            try:
                d = _parse_date(r.get("day", ""))
                if d not in days:
                    continue
                s_map[d] = {"o": float(r["open"]), "c": float(r["close"]),
                            "h": float(r["high"]), "l": float(r["low"]),
                            "v": float(r["volume"])}
            except (TypeError, ValueError, KeyError, AttributeError):
                continue
    out = []
    for d in sorted(days):
        t = t_map.get(d)
        s = s_map.get(d)
        if s is not None and (prefer == "sina" or t is None):
            o, c, h, lo, v = s["o"], s["c"], s["h"], s["l"], s["v"]
        elif t is not None:
            o, c, h, lo, v = t
        else:
            continue
        row = _row_frame(symbol, d, o, h, lo, c, v, vol_divisor)
        if row is not None:
            out.append(row)
    if not out:
        # 两源都只返回空（停牌等）→ 空帧；网络失败（s_rows is None 且 t 空）→ None
        if s_rows is None and not t_map:
            return None
        return pl.DataFrame(schema=_DAY_SCHEMA)
    return pl.DataFrame(out, schema_overrides={"date": pl.Date})


def backfill_days(symbols: list[tuple[str, float]], days: list[_date],
                  progress: str = "备用日线回源",
                  prefer: str = "tencent") -> dict:
    """多标的日线回补。symbols: [(tf_symbol, vol_divisor)]，其中 divisor
    股票=100.0（分区 volume=手），ETF/指数=1.0（分区 volume=股）。

    返回 {"frames": {day: [pl.DataFrame]}, "ok_symbols": [...],
    "uncovered": [...]}。
    """
    day_set = set(days)
    frames_by_day: dict[_date, list[pl.DataFrame]] = {d: [] for d in days}
    ok_symbols: list[str] = []
    uncovered: list[str] = []
    local = threading.local()
    _drain_fetch_errors()  # 起点重置：上轮残留不计入本轮汇总

    def _session() -> requests.Session:
        s = getattr(local, "s", None)
        if s is None:
            s = requests.Session()
            local.s = s
        return s

    def _one(item: tuple[str, float]):
        sym, divisor = item
        try:
            out = build_symbol_frame(sym, divisor, day_set, _session(),
                                     prefer=prefer)
            if _pacing():
                time.sleep(_pacing())
            return sym, out
        except Exception as e:
            logger.debug("备用日线 %s 异常: %s", sym, e)
            return sym, None

    total = len(symbols)
    done = {"n": 0}
    with ThreadPoolExecutor(max_workers=_workers(),
                            thread_name_prefix="alt-daily") as ex:
        futures = [ex.submit(_one, it) for it in symbols]
        for fut in futures:
            sym, frame = fut.result()
            done["n"] += 1
            if done["n"] % 1000 == 0 or done["n"] == total:
                logger.info("%s 进度 %d/%d", progress, done["n"], total)
            if frame is None or frame.is_empty():
                uncovered.append(sym)
                continue
            ok_symbols.append(sym)
            for d in days:
                sub = frame.filter(pl.col("date") == d)
                if not sub.is_empty():
                    frames_by_day[d].append(sub)
    errs = _drain_fetch_errors()
    if errs:
        counts = Counter(errs)
        detail = ", ".join(f"{src} {msg} ×{n}"
                           for (src, msg), n in counts.most_common(8))
        logger.warning("%s: 备用源请求失败汇总: %s（共 %d 次，未覆盖 %d/%d 只）",
                       progress, detail, len(errs), len(uncovered), total)
    return {"frames": frames_by_day, "ok_symbols": ok_symbols,
            "uncovered": uncovered}


def _day_partition_symbols(root, day: _date) -> set[str]:
    """读某日日线分区已落盘 symbol 集（仅 symbol 列；兼容 ETF 无 date 列格式）。"""
    part = root / f"date={day}" / "part.parquet"
    if not part.exists():
        return set()
    try:
        return set(pl.read_parquet(part, columns=["symbol"])["symbol"].to_list())
    except Exception:
        return set()


def _sync_daily(symbols: list[tuple[str, float]], days: list[_date], root,
                label: str, prefer: str = "tencent",
                overwrite: bool = False) -> dict:
    """通用落盘：only_missing 按日过滤 + _write_daily_partition 逐日 merge。

    ``overwrite`` 为真时跳过已有过滤（审计发现口径污染后的覆盖重填用，
    merge keep-last 让新帧获胜）。
    """
    from app.services import mootdx_service as ms
    days = [d for d in days if d <= _dt.date.today()]
    if not days or not symbols:
        return {"rows": {}, "total": 0, "uncovered": [], "source": "alt"}
    per_day_have: dict[_date, set[str]] = {}
    if not overwrite:
        per_day_have = {d: _day_partition_symbols(root, d) for d in days}
        have_all = (set.intersection(*per_day_have.values())
                    if per_day_have else set())
        skipped = [s for s, _ in symbols if s in have_all]
        symbols = [it for it in symbols if it[0] not in have_all]
        if skipped:
            logger.info("alt_daily: %s %d 只各目标日均有数据，跳过", label, len(skipped))
    if not symbols:
        return {"rows": {d.isoformat(): 0 for d in days}, "total": 0,
                "uncovered": [], "source": "alt"}
    res = backfill_days(symbols, days, progress=f"备用{label}日线回源",
                        prefer=prefer)
    rows: dict[str, int] = {}
    total = 0
    for d in days:
        have = per_day_have.get(d, set())
        frames = [f for f in res["frames"][d]
                  if f["symbol"][0] not in have]
        if not frames:
            rows[d.isoformat()] = 0
            continue
        out = pl.concat(frames).unique(
            subset=["symbol", "date"], keep="last").sort(["symbol", "date"])
        before = out.height
        ms._write_daily_partition(out, root)
        rows[d.isoformat()] = before
        total += before
    logger.warning("alt_daily: %s日线回补完成 %d 行，未覆盖 %d 只",
                   label, total, len(res["uncovered"]))
    return {"rows": rows, "total": total, "uncovered": res["uncovered"],
            "source": "alt"}


def sync_stock_daily_alt(days: list[_date], symbols: list[str] | None = None,
                         prefer: str = "tencent",
                         overwrite: bool = False) -> dict:
    """备用回补股票日线（kline_daily，volume 手）。

    ``symbols`` 指定标的子集（审计覆盖重填用）；None = 全宇宙。
    """
    from app.services import mootdx_service as ms
    stocks = [s for s in ms._stock_universe() if not s.endswith(".BJ")]
    if symbols is not None:
        want = set(symbols)
        stocks = [s for s in stocks if s in want]
    if not stocks:
        logger.warning("alt_daily: 股票宇宙为空，跳过")
        return {"rows": {}, "total": 0, "uncovered": [], "source": "alt"}
    listing = ms._listing_date_map()
    earliest = min(days) if days else _dt.date.today()
    todo = []
    for s in stocks:
        if s not in listing or (_date(1970, 1, 1) < listing[s] <= earliest):
            todo.append((s, 100.0))
    return _sync_daily(todo, days, ms.STOCK_DAILY_ROOT, "股票",
                       prefer=prefer, overwrite=overwrite)


def sync_etf_daily_alt(days: list[_date], symbols: list[str] | None = None,
                       prefer: str = "tencent",
                       overwrite: bool = False) -> dict:
    """备用回补 ETF 日线（kline_etf_daily，volume 股）。参数同股票侧。"""
    from app.services import mootdx_service as ms
    codes = ms._etf_universe()
    tf_syms = []
    for jq in codes:
        try:
            pure, mkt = jq.split(".")
            tf_syms.append((pure + (".SH" if mkt == "XSHG" else ".SZ"), 1.0))
        except ValueError:
            continue
    if symbols is not None:
        want = set(symbols)
        tf_syms = [it for it in tf_syms if it[0] in want]
    if not tf_syms:
        logger.warning("alt_daily: ETF 宇宙为空，跳过")
        return {"rows": {}, "total": 0, "uncovered": [], "source": "alt"}
    return _sync_daily(tf_syms, days, ms.ETF_DAILY_ROOT, "ETF",
                       prefer=prefer, overwrite=overwrite)


def sync_index_daily_alt(days: list[_date], symbols: list[str] | None = None,
                         prefer: str = "tencent",
                         overwrite: bool = False) -> dict:
    """备用回补指数日线（kline_index_daily，volume 股）。参数同股票侧。

    指数宇宙已是 .SH/.SZ 分区格式，直接可用。
    """
    from app.services import mootdx_service as ms
    indices = [s for s in ms._index_universe() if not s.endswith(".BJ")]
    if symbols is not None:
        want = set(symbols)
        indices = [s for s in indices if s in want]
    if not indices:
        logger.warning("alt_daily: 指数宇宙为空，跳过")
        return {"rows": {}, "total": 0, "uncovered": [], "source": "alt"}
    todo = [(s, 1.0) for s in indices]
    return _sync_daily(todo, days, ms.INDEX_DAILY_ROOT, "指数",
                       prefer=prefer, overwrite=overwrite)


def fill_recent_gaps_daily(kind: str = "stock", lookback: int = 5,
                           force: bool = False) -> dict | None:
    """近期日线缺口的备用链兜底（调度侧唯一入口；健康时零开销 no-op）。

    缺口 = 分区缺失的已收盘交易日；同时满足才回补：
    1. 近 ``lookback`` 天内有缺口；
    2. K 线熔断开路（mootdx/TickFlow 当轮已确认不可用）。
    ``force=True`` 跳过条件 2（运维手动补跑用，默认关闭）。
    """
    from app.quant.jqengine.datasource.mootdx_breaker import kline_allowed
    from app.services import mootdx_service as ms
    if kind not in ("stock", "etf", "index"):
        raise ValueError(f"未知 kind: {kind}")
    roots = {"etf": ms.ETF_DAILY_ROOT, "index": ms.INDEX_DAILY_ROOT}
    root = roots.get(kind, ms.STOCK_DAILY_ROOT)
    missing = [d for d in ms._missing_daily_days(root)][-lookback:]
    if not missing:
        return None
    if kline_allowed() and not force:
        logger.debug("alt_daily: %s日线缺口 %s 但熔断关闭，主源自行处理",
                     kind, [d.isoformat() for d in missing])
        return None
    logger.warning("alt_daily: %s日线缺口 %s 且%s，切备用链回补",
                   kind, [d.isoformat() for d in missing],
                   "force 绕过熔断状态" if force else "熔断开路")
    if kind == "etf":
        return sync_etf_daily_alt(missing)
    if kind == "index":
        return sync_index_daily_alt(missing)
    return sync_stock_daily_alt(missing)
