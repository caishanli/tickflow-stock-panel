"""mootdx 数据服务：独立于模拟盘，由盘后管道调度触发。

负责两类每日收盘落盘更新（与「每日分钟线」同机制）：
1. ETF 分钟分区（``data/kline_etf_minute/date=YYYY-MM-DD/part.parquet``）：
   收盘后把当日全部 ETF 的真实 1m 从 mootdx 拉取落盘，供回测/模拟盘复用，
   避免每次回测联网回源。
2. ETF 前复权因子表（``data/adj_factor_etf/all.parquet``）：
   用 mootdx xdxr 除权除息记录重建逐日因子，增量合并进既有表。

回测、模拟盘都只读落盘数据（DataManager._adj_factor_map /
_load_minute_from_partitions），本服务不参与策略执行。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import time
from datetime import date as _date
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from app.quant.jqengine.datasource.manager import DataManager
from app.quant.jqengine.datasource.mootdx_src import MootdxSource

logger = logging.getLogger("app.services.mootdx_service")

_env_root = os.getenv("PARTITION_DATA_ROOT", "").strip()
if _env_root:
    DATA_ROOT = Path(_env_root)
else:
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))
    DATA_ROOT = Path(_repo_root) / "data"
ETF_MINUTE_ROOT = DATA_ROOT / "kline_etf_minute"
ADJ_FACTOR_PATH = DATA_ROOT / "adj_factor_etf" / "all.parquet"
INDEX_DAILY_ROOT = DATA_ROOT / "kline_index_daily"
# 回源失败标的落盘（symbol, 原因, 时间），供用户手动核查
FAILURE_LOG_PATH = DATA_ROOT / "mootdx_sync_failures.csv"

# 只处理 2020 年以来的除权事件（回测窗口有限，太早的因子无意义）
_SINCE_YEAR = 2020

# ETF 日线内容完整性校验阈值：分区 symbol 覆盖率低于该值即判残缺重写。
# 宇宙缺陷/真实退市不会把覆盖率打到 0.5 以下（全市场 1658 只正常全落盘）。
_ETF_DAILY_MIN_COVERAGE = 0.5
# 内容校验只看最近 N 个分区（早年分区无必要逐日读文件，性能考虑）。
_ETF_DAILY_RECENT_LIMIT = 30
# 股票分钟内容完整性校验阈值：分区 symbol 覆盖率低于该值即判残缺重写；
# 可由环境变量覆盖（STOCK_MINUTE_MIN_COVERAGE=0.3），默认 0.5。
_STOCK_MINUTE_MIN_COVERAGE = float(os.getenv("STOCK_MINUTE_MIN_COVERAGE", "0.5"))
# 股票分钟内容校验看最近 N 个分区（全市场 ~5200 只，逐日读 symbol 列成本更高，
# 只看近窗口）。可由 STOCK_MINUTE_RECENT_LIMIT 覆盖。
_STOCK_MINUTE_RECENT_LIMIT = int(os.getenv("STOCK_MINUTE_RECENT_LIMIT", "30"))
# 权威 ETF 代码段（对齐聚宽 get_all_securities(['etf']) 名单段分布）。
# 深市 159/161/169/180/181；沪市 501/506/510~518/520/526/530/551/560~563/588/589。
# 每个段在完整名单中至少出现 1 只；宇宙缺失整个段 = 快照/回源异常（如 501018
# 南方原油、161226 白银 LOF 曾因 _is_jq_etf_code 误过滤整段消失）。
_ETF_UNIVERSE_EXPECTED_SEGMENTS = (
    "159", "161", "169", "180", "181",
    "501", "506",
    "510", "511", "512", "513", "515", "516", "517", "518",
    "520", "526", "530", "551",
    "560", "561", "562", "563",
    "588", "589",
)


def _append_failure(sym: str, reason: str) -> None:
    """把回源失败标的追加到 failure csv（symbol, 原因, 时间）。"""
    try:
        from datetime import datetime as _dt
        line = f"{sym},{reason},{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        with open(FAILURE_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 失败记录写入失败: %s", sym)


# 后台回源节流：客户端请求优先占用 mootdx socket/CPU，backfill 每取 N 个 symbol
# 主动 sleep 一小段让出资源。默认开启（每 5 个 symbol 睡 0.2s），环境变量可调。
_BACKFILL_THROTTLE_EVERY = int(os.getenv("BACKFILL_THROTTLE_EVERY", "5"))
_BACKFILL_THROTTLE_SLEEP = float(os.getenv("BACKFILL_THROTTLE_SLEEP", "0.2"))
# 盘中让路降速（A 股交易时段）：服务器侧连接/频率配额是共享瓶颈，历史回源
# 密集请求会把实时拉取挤到 30s 墙钟超时（08-13 盘中回源致活跃 ETF 分钟取数
# 失败被误判停牌）。盘中每 1 个 symbol 睡 1s（约 1 req/s），把配额让给实时
# 16-worker 共享池；收盘后恢复原节奏。环境变量可调（BACKFILL_INTRADAY_EVERY=0
# 完全禁流，等价旧行为）。
_BACKFILL_INTRADAY_EVERY = int(os.getenv("BACKFILL_INTRADAY_EVERY", "1"))
_BACKFILL_INTRADAY_SLEEP = float(os.getenv("BACKFILL_INTRADAY_SLEEP", "1.0"))


def _is_market_open(now: _dt.datetime | None = None) -> bool:
    """A 股交易时段判定（口径同 stockdata.sources._in_trading）。"""
    now = now or _dt.datetime.now()
    t = now.time()
    return (now.weekday() < 5
            and (_dt.time(9, 30) <= t <= _dt.time(11, 30)
                 or _dt.time(13, 0) <= t <= _dt.time(15, 0)))


def _throttle_backfill(i: int) -> None:
    """后台回源节流：每 ``_BACKFILL_THROTTLE_EVERY`` 个 symbol 后 sleep 一小段。

    ``i`` 为当前循环序号（0-based）。``sleep`` 主动释放 GIL 与 mootdx socket
    占用，给客户端实时请求让路（客户端走共享线程池，不受本函数影响）。
    仅在后台回源路径调用（sync_daily/sync_index_daily/sync_stock_minute/…）。

    盘中（交易时段）使用独立的降速参数：mootdx 服务器对每 IP 的连接/请求
    频率有限制，历史回源密集请求会把实时拉取挤到墙钟超时（08-13 13:55 事故
    根因），盘中每只 symbol 睡 ``_BACKFILL_INTRADAY_SLEEP`` 秒让出配额。
    """
    if _is_market_open():
        if _BACKFILL_INTRADAY_EVERY <= 0:
            return
        if (i + 1) % _BACKFILL_INTRADAY_EVERY == 0:
            time.sleep(_BACKFILL_INTRADAY_SLEEP)
        return
    if _BACKFILL_THROTTLE_EVERY <= 0:
        return
    if (i + 1) % _BACKFILL_THROTTLE_EVERY == 0:
        time.sleep(_BACKFILL_THROTTLE_SLEEP)


def _to_tf_symbol(code: str) -> str:
    """JQ 代码 (.XSHG/.XSHE) -> 分区 symbol (.SH/.SZ)。"""
    pure, mkt = code.split(".")
    return pure + (".SH" if mkt == "XSHG" else ".SZ")


def _etf_universe() -> list[str]:
    """返回 ETF 宇宙 JQ codes 列表（优先快照，回退分区里已有的标的）。"""
    snap_path = DATA_ROOT / "quant_kline" / "etf_universe_snapshot.json"
    if snap_path.exists():
        import json
        try:
            codes = json.loads(snap_path.read_text()).get("codes", [])
            if codes:
                return sorted(codes)
        except Exception as e:
            logger.warning("ETF 快照读取失败，回退分区标的: %s", e)
    lf = pl.scan_parquet(str(ETF_MINUTE_ROOT / "**" / "*.parquet"),
                         hive_partitioning=True)
    syms = lf.select("symbol").unique().collect()["symbol"].to_list()
    out = []
    for s in syms:
        pure, mkt = s.split(".")
        out.append(pure + (".XSHG" if mkt == "SH" else ".XSHE"))
    return sorted(set(out))


def sync_etf_minute(day: _date | None = None) -> int:
    """收盘后同步指定交易日（默认今天）全部 ETF 分钟到按日分区。

    逐标的拉真实 1m：近期日（≤5 天）用 ``get_minute_recent``（含当日盘中），
    历史日（>5 天）用 ``get_minute`` 全量拉再过滤当日（支持 4/1 起缺失日回补）。
    以 ``date={day}/part.parquet`` 原子写盘。返回写入行数。
    """
    day = day or _date.today()
    src = MootdxSource()
    codes = _etf_universe()
    if not codes:
        logger.warning("mootdx_service: ETF 宇宙为空，跳过分钟同步")
        return 0
    historical = (day < _date.today() - _dt.timedelta(days=5))
    frames = []
    for i, jq in enumerate(codes):
        _throttle_backfill(i)
        try:
            if historical:
                df = src.get_minute(jq, max_bars=40000)
            else:
                df = src.get_minute_recent(jq, pages=2)
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", jq, e)
            continue
        if df is None or df.empty:
            continue
        df = df.copy()
        df["symbol"] = _to_tf_symbol(jq)
        df = df.reset_index()
        keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date == day]
        if df.empty:
            continue
        frames.append(pl.from_pandas(df))
        if (i + 1) % 500 == 0:
            try:
                src._client = None
                src._server_idx = -1
            except Exception:  # noqa: BLE001
                pass
    if not frames:
        return 0
    out = pl.concat(frames).unique(
        subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
    out = out.with_columns(
        pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
    return _write_minute_partition(out, ETF_MINUTE_ROOT, day)


def _write_minute_partition(df: pl.DataFrame, root: Path, day: _date) -> int:
    """按 date 分区原子写分钟（读旧→concat→unique→tmp→rename）。返回行数。"""
    pdir = root / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    tmp = pdir / "part.tmp"
    if part.exists():
        old = pl.read_parquet(part)
        df = pl.concat([old, df]).unique(
            subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
    df = df.sort(["symbol", "datetime"])
    df.write_parquet(tmp)
    tmp.rename(part)
    logger.info("mootdx_service: 分钟落盘 %d 行 → %s", df.height, part)
    return df.height


STOCK_MINUTE_ROOT = DATA_ROOT / "kline_minute"
# 股票分钟回源起点：从该日起补全市场分钟（用户需求 4/1 起）
STOCK_MINUTE_START = _date(2026, 4, 1)
# 每攒满多少只股票一次性批量写分区（写盘 IO 与内存的折中）
_STOCK_MINUTE_BATCH = 100
# 调度任务单次回源的股票分钟只数上限（增量慢跑：启动线程与盘后 cron 各跑
# 一批，resume 跳过已覆盖，多轮后自动补齐全部缺口；None = 一次拉全量）
STOCK_MINUTE_BATCH_LIMIT = 20


def _flush_stock_minute_chunk(chunk: list[pl.DataFrame]) -> None:
    """把一批股票的分钟 bar 按日期分区一次性合并写入。

    先按交易日分组聚合 chunk 内所有股票，再对每个日期分区读旧→concat→
    unique→原子替换。避免逐股票逐分区 IO。
    """
    if not chunk:
        return
    all_df = pl.concat(chunk).unique(
        subset=["symbol", "datetime"], keep="last")
    all_df = all_df.with_columns(pl.col("datetime").dt.date().alias("_day"))
    for d, g in all_df.group_by("_day"):
        day = d[0]
        g = g.drop("_day").sort(["symbol", "datetime"])
        pdir = STOCK_MINUTE_ROOT / f"date={day}"
        pdir.mkdir(parents=True, exist_ok=True)
        pfile = pdir / "part.parquet"
        tmp = pdir / "part.tmp"
        if pfile.exists():
            old = pl.read_parquet(pfile)
            merged = pl.concat([old, g]).unique(
                subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
        else:
            merged = g
        merged.write_parquet(tmp)
        tmp.rename(pfile)


def _existing_minute_symbols() -> set[str]:
    """收集**最新**股票分钟分区里已落盘的 symbol 集合（用于 resume）。

    只读最新日期分区：若某标的只存在于老分区而最新分区缺失（如上次回源
    中途中断），不能误判为"已覆盖"而跳过 —— 否则最新交易日数据永久缺失。
    """
    if not STOCK_MINUTE_ROOT.is_dir():
        return set()
    days = sorted(STOCK_MINUTE_ROOT.glob("date=*"))
    if not days:
        return set()
    latest = days[-1] / "part.parquet"
    if not latest.exists():
        return set()
    try:
        df = pl.read_parquet(latest, columns=["symbol"])
        return set(df["symbol"].to_list())
    except Exception:  # noqa: BLE001
        return set()


def _listing_date_map() -> dict[str, _date]:
    """返回 {symbol: 上市日期} 映射（instruments parquet，缺失回退 4/1 起点）。

    新股上市日可能晚于回源起点：4/1 后上市的股票，其最早日线/分钟从上市日
    才开始，按上市日作为该标的的回源起点，避免对上市前的空窗期拉取（必然
    获取不到，浪费 8-10s/次服务器轮换超时）。
    """
    inst_path = DATA_ROOT / "instruments" / "instruments.parquet"
    out: dict[str, _date] = {}
    if not inst_path.exists():
        return out
    try:
        df = pl.read_parquet(inst_path, columns=["symbol", "listing_date"])
        for row in df.iter_rows():
            sym, ld = row
            if sym is None or ld is None:
                continue
            try:
                s = str(ld).strip()
                if not s:
                    continue
                if "-" in s:
                    d = _date.fromisoformat(s[:10])
                else:
                    d = _date(int(s[:4]), int(s[4:6]), int(s[6:8]))
                out[sym] = d
            except Exception:  # noqa: BLE001
                continue
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 上市日期读取失败: %s", e)
    return out


def sync_stock_minute(limit: int | None = None) -> int:
    """回源 4/1 起全市场 A 股分钟到 ``kline_minute`` 分区（一次拉全量多日）。

    对每只股票用 mootdx ``get_minute`` 拉一次全量历史 1m（约 3 个月，含
    4/1 至今），过滤掉 4/1 前的 bar 后按交易日分组缓存；每攒满
    ``_STOCK_MINUTE_BATCH`` 只即把累积 bar 一次性分散写入各 ``date=`` 分区
    （读旧→concat→unique keep=last）。跳过北交所（mootdx 无数据）。

    ``limit``：最多本次处理的股票数（用于调度任务增量慢跑——每批拉一小
    部分，resume 自动跳过已覆盖，多轮后自然补齐全部缺口）。None = 一次
    拉全量 todo。返回写入行数。

    分块批量写：避免每只股票逐分区读-改-写（5205 只 × 84 分区 IO 巨大），
    批量合并把写盘次数降一个量级。返回写入行数。

    注意：全市场 ~5200 只 × 3 个月历史约 1.5-2 小时，建议后台线程调用；
    调度场景传 ``limit`` 分批慢跑。
    """
    src = MootdxSource()
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("mootdx_service: 股票宇宙为空，跳过分钟同步")
        return 0
    listing = _listing_date_map()
    # resume：收集已落盘的 symbol，跳过（中断后续跑不重拉）
    done_syms = _existing_minute_symbols()
    todo = [s for s in stocks if s not in done_syms]
    if not todo:
        logger.info("mootdx_service: 股票分钟已全部覆盖（%d 只），无需回源", len(done_syms))
        return 0
    if limit is not None:
        todo = todo[:limit]
    # 上市日期占位（1970-01-01 = instruments 退市/异常数据）：这些标的 4/1 前
    # 已无交易，回源必然获取不到（每次 8-10s 服务器轮换超时），直接跳过。
    pre_delisted = [s for s in todo if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("mootdx_service: 跳过 %d 只退市/异常标的（上市日期占位）: %s",
                    len(pre_delisted), pre_delisted[:10])
        todo = [s for s in todo if s not in set(pre_delisted)]
    logger.info("mootdx_service: 股票分钟回源 %d/%d（跳过已覆盖 %d）",
                len(todo), len(stocks), len(done_syms))
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    total = 0
    chunk: list[pl.DataFrame] = []
    for i, sym in enumerate(todo):
        _throttle_backfill(i)
        # 回源起点 = max(全局起点, 该股上市日)；新股上市前无数据，不提前拉
        sym_start = STOCK_MINUTE_START
        ld = listing.get(sym)
        if ld is not None and ld > sym_start:
            sym_start = ld
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000)
        except TimeoutError:
            # 超时：坏连接状态，整个重建 src（避免坏 socket/server 索引残留）
            src = MootdxSource()
            _append_failure(sym, "timeout")
            continue
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", sym, e)
            _append_failure(sym, f"exception:{str(e)[:60]}")
            continue
        if df is None or df.empty:
            _append_failure(sym, "empty")
            continue
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date >= sym_start]
        if df.empty:
            _append_failure(sym, f"no_data_since_{sym_start}")
            continue
        sub = pl.from_pandas(df)
        sub = sub.with_columns(pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
        sub = sub.unique(subset=["symbol", "datetime"], keep="last")
        chunk.append(sub)
        total += sub.height
        if len(chunk) >= _STOCK_MINUTE_BATCH:
            _flush_stock_minute_chunk(chunk)
            chunk = []
            logger.info("mootdx_service: 股票分钟 %d/%d 处理, 累计 %d 行",
                        i + 1, len(todo), total)
            # 每批换新连接：彻底刷新 socket/server 状态，防长批连接劣化
            src = MootdxSource()
    if chunk:
        _flush_stock_minute_chunk(chunk)
    logger.info("mootdx_service: 股票分钟回源完成, 累计 %d 行", total)
    return total


def sync_stock_minute_day(day: _date) -> int:
    """按缺失日补全全市场股票分钟到 ``kline_minute/date={day}``。

    「有数据才回」：上市日晚于目标日的 symbol 跳过（该日尚未上市）；
    当日停牌/无 bar 自然跳过不落盘。逐 symbol 用 ``get_minute`` 全量拉，
    过滤到 ``day`` 后批量写分区（复用 ``_flush_stock_minute_chunk``）。
    返回写入行数。
    """
    src = MootdxSource()
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("mootdx_service: 股票宇宙为空，跳过分钟同步")
        return 0
    listing = _listing_date_map()
    # 上市日期占位（1970-01-01 = instruments 退市/异常数据）：这些标的已无交易，
    # 回源必然获取不到（每次 8-10s 服务器轮换超时），直接跳过（与 sync_stock_minute 一致）。
    pre_delisted = [s for s in stocks if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("mootdx_service: 跳过 %d 只退市/异常标的（上市日期占位）: %s",
                    len(pre_delisted), pre_delisted[:10])
        stocks = [s for s in stocks if s not in set(pre_delisted)]
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    total = 0
    chunk: list[pl.DataFrame] = []
    for i, sym in enumerate(stocks):
        _throttle_backfill(i)
        ld = listing.get(sym)
        if ld is not None and ld > day:
            continue  # 上市晚于目标日，该日无数据
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000)
        except TimeoutError:
            src = MootdxSource()
            _append_failure(sym, "timeout")
            continue
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", sym, e)
            _append_failure(sym, f"exception:{str(e)[:60]}")
            continue
        if df is None or df.empty:
            _append_failure(sym, "empty")
            continue
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date == day]
        if df.empty:
            continue  # 当日停牌/无 bar，跳过
        sub = pl.from_pandas(df)
        sub = sub.with_columns(pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
        sub = sub.unique(subset=["symbol", "datetime"], keep="last")
        chunk.append(sub)
        total += sub.height
        if len(chunk) >= _STOCK_MINUTE_BATCH:
            _flush_stock_minute_chunk(chunk)
            chunk = []
            src = MootdxSource()
    if chunk:
        _flush_stock_minute_chunk(chunk)
    logger.info("mootdx_service: 股票分钟按日回源 %s 完成, %d 行", day, total)
    return total


def sync_stock_minute_range(days: list[_date]) -> int:
    """批量回补多个缺失交易日：每只拉一次全量历史，一次写入所有缺失日分区。

    与 ``sync_stock_minute_day``（逐缺失日全市场重拉）不同，多日缺口时每个
    symbol 只拉一次 ``get_minute`` 全量，再按 ``day_set`` 过滤出落在缺失日的
    bar，由 ``_flush_stock_minute_chunk`` 按各自交易日分组写入 ``date=`` 分区
    （读旧→concat→unique），避免 O(N_days × 全市场) 的重复回源。
    跳过退市/异常（上市日期 1970 占位）与上市日晚于整个缺失窗口末端的标的。
    返回写入行数。
    """
    day_set = set(days)
    window_end = max(days)
    src = MootdxSource()
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("mootdx_service: 股票宇宙为空，跳过分钟同步")
        return 0
    listing = _listing_date_map()
    # 上市日期占位（1970-01-01 = instruments 退市/异常数据）：这些标的已无交易，
    # 回源必然获取不到（每次 8-10s 服务器轮换超时），直接跳过。
    pre_delisted = [s for s in stocks if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("mootdx_service: 跳过 %d 只退市/异常标的（上市日期占位）: %s",
                    len(pre_delisted), pre_delisted[:10])
        stocks = [s for s in stocks if s not in set(pre_delisted)]
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    total = 0
    chunk: list[pl.DataFrame] = []
    for i, sym in enumerate(stocks):
        _throttle_backfill(i)
        ld = listing.get(sym)
        if ld is not None and ld > window_end:
            continue  # 上市晚于整个缺失窗口，窗口内无数据
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000)
        except TimeoutError:
            src = MootdxSource()
            _append_failure(sym, "timeout")
            continue
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", sym, e)
            _append_failure(sym, f"exception:{str(e)[:60]}")
            continue
        if df is None or df.empty:
            _append_failure(sym, "empty")
            continue
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date.isin(day_set)]
        if df.empty:
            continue  # 缺失窗口内无 bar，跳过
        sub = pl.from_pandas(df)
        sub = sub.with_columns(pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
        sub = sub.unique(subset=["symbol", "datetime"], keep="last")
        chunk.append(sub)
        total += sub.height
        if len(chunk) >= _STOCK_MINUTE_BATCH:
            _flush_stock_minute_chunk(chunk)
            chunk = []
            src = MootdxSource()
    if chunk:
        _flush_stock_minute_chunk(chunk)
    logger.info("mootdx_service: 股票分钟批量回源 %d 个缺失日, %d 行", len(days), total)
    return total


def backfill_missing_partitions(missing: dict[str, list[_date]]) -> dict:
    """逐缺失日复用现有 sync 函数补全，单日失败记 errors 不阻断。

    ``missing`` 键名与 ``scan_missing_partitions`` 一致：
    kline_daily / kline_etf_daily → sync_daily；kline_index_daily →
    sync_index_daily；kline_etf_minute → sync_etf_minute；
    kline_minute → sync_stock_minute_range（批量一次补全全部缺失日）。
    """
    result: dict = {
        "daily_days": [], "index_daily_days": [],
        "etf_minute_days": [], "stock_minute_days": [],
        "etf_nav_days": [], "errors": [],
    }

    for day in missing.get("kline_daily", []) + missing.get("kline_etf_daily", []):
        try:
            sync_daily(day)
            result["daily_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"daily {day}: {e}")
    for day in missing.get("kline_index_daily", []):
        try:
            sync_index_daily(day)
            result["index_daily_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"index_daily {day}: {e}")
    for day in missing.get("kline_etf_minute", []):
        try:
            sync_etf_minute(day)
            result["etf_minute_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"etf_minute {day}: {e}")
    min_days = missing.get("kline_minute", [])
    if min_days:
        try:
            sync_stock_minute_range(min_days)
            result["stock_minute_days"].extend(str(d) for d in min_days)
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"stock_minute {min_days}: {e}")

    for day in missing.get("etf_nav", []):
        try:
            from app.services import etf_nav_service
            etf_nav_service.sync_etf_nav(day)
            result["etf_nav_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"etf_nav {day}: {e}")

    return result


def scan_and_backfill_full() -> dict:
    """00:00 全量巡检 + 补全入口：扫描缺失 → 逐日补全 → 汇总。"""
    missing = scan_missing_partitions()
    backfilled = backfill_missing_partitions(missing)
    total = sum(len(v) for v in missing.values())
    msg = "mootdx_service: 全量扫描 %d 缺失日, 补全 %s, errors=%s"
    args = (total, {k: len(v) for k, v in backfilled.items()},
            len(backfilled["errors"]))
    if backfilled["errors"]:
        # 补全后有残留错误 → WARNING（供钉钉通知等消费）
        logger.warning(msg, *args)
    else:
        logger.info(msg, *args)
    return {"missing": missing, "backfilled": backfilled,
            "errors": backfilled["errors"]}


def sync_adj_factor() -> dict:
    """增量更新 ETF 前复权因子表（mootdx xdxr 事件重建）。

    对宇宙内每只有除权事件的标的，用 xdxr 记录 + 日线 close 重建逐日
    ex_factor 序列，覆盖该标的在 ``all.parquet`` 中的行（全量重算该标的，
    幂等）。返回 {written_symbols, rows, total_symbols}。
    """
    src = MootdxSource()
    dm = DataManager()
    daily = dm._load_daily_from_partitions(asof=None)
    codes = _etf_universe()
    if not codes:
        return {"written_symbols": 0, "rows": 0, "total_symbols": 0}
    # 只保留宇宙内的标的
    daily = {k: v for k, v in daily.items() if k in set(codes)}
    frames = []
    for jq, pdf in daily.items():
        closes = pdf["close"].dropna()
        if closes.empty:
            continue
        rows = src._xdxr_rows(jq.split(".")[0])
        events = []
        for r in (rows or []):
            cat = r.get("category")
            year = r.get("year")
            if not year or int(year) < _SINCE_YEAR:
                continue
            try:
                ex_dt = pd.Timestamp(int(r["year"]), int(r["month"]), int(r["day"]))
            except Exception:
                continue
            if cat == 11:
                suogu = float(r.get("suogu") or 0)
                if suogu <= 0:
                    continue
                events.append((ex_dt, 1.0 / suogu))
            elif cat == 1:
                # 除权参考价公式与 mootdx_src._to_qfq 同口径：
                # ex_price = (prev_close - fh + pgj*pg) / (1+sg+pg)
                # factor = ex_price / prev_close。现金红利(fh)/配股会摊薄价格，
                # 纯送转比例式 1/(1+sg+pg) 会漏掉这两项（对齐 bug：510880 等
                # 有年度现金分红，漏算导致前复权价偏离聚宽）。
                fh = float(r.get("fenhong") or 0) / 10.0      # 每股现金红利(元)
                sg = float(r.get("songzhuangu") or 0) / 10.0  # 每股送转
                pg = float(r.get("peigu") or 0) / 10.0        # 每股配股
                pgj = float(r.get("peigujia") or 0)           # 配股价
                if fh == 0 and sg == 0 and pg == 0:
                    continue
                prev = closes.loc[closes.index < ex_dt].dropna()
                if prev.empty:
                    continue  # 除权日前一收盘价不在帧内，因子无法计算
                prev_close = float(prev.iloc[-1])
                if prev_close <= 0:
                    continue
                ex_price = (prev_close - fh + pgj * pg) / (1.0 + sg + pg)
                if ex_price <= 0:
                    continue
                events.append((ex_dt, ex_price / prev_close))
        if not events:
            continue
        events = [(e, f) for e, f in events if e < closes.index.max()]
        if not events:
            continue
        adj = pd.Series(1.0, index=closes.index)
        for ex_dt, f in events:
            adj.loc[adj.index < ex_dt] *= f
        frames.append(pl.DataFrame({
            "symbol": jq,
            "trade_date": [d.isoformat() for d in closes.index.date],
            "ex_factor": adj.values,
        }))
    if not frames:
        logger.info("mootdx_service: 无除权事件，因子表无更新")
        return {"written_symbols": 0, "rows": 0, "total_symbols": len(codes)}
    out = pl.concat(frames)
    out = out.with_columns(pl.col("trade_date").cast(pl.Date))
    out = out.unique(subset=["symbol", "trade_date"], keep="last").sort(
        ["symbol", "trade_date"])
    ADJ_FACTOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    # 合并既有表（幂等）：同 symbol+date 覆盖，其余保留
    if ADJ_FACTOR_PATH.exists():
        old = pl.read_parquet(ADJ_FACTOR_PATH)
        out = pl.concat([old, out]).unique(
            subset=["symbol", "trade_date"], keep="last").sort(["symbol", "trade_date"])
    tmp = ADJ_FACTOR_PATH.parent / "all.tmp.parquet"
    out.write_parquet(tmp)
    tmp.rename(ADJ_FACTOR_PATH)
    n_syms = out["symbol"].n_unique()
    logger.info("mootdx_service: 因子表更新 %d 行 / %d 只 → %s",
                out.height, n_syms, ADJ_FACTOR_PATH)
    return {"written_symbols": len(frames), "rows": out.height,
            "total_symbols": len(codes)}


# ---------------------------------------------------------------------------
# 启动回源：系统启动时补齐到当前时间缺失的分钟线和日线
# ---------------------------------------------------------------------------

STOCK_DAILY_ROOT = DATA_ROOT / "kline_daily"
ETF_DAILY_ROOT = DATA_ROOT / "kline_etf_daily"
_DAILY_BACKFILL_LIMIT_DAYS = 90   # 启动回源最多往前补的天数（避免全历史重拉）


def _partition_dates(root: Path) -> list[str]:
    """返回某分区目录下全部 date=YYYY-MM-DD 列表（升序）。"""
    if not root.is_dir():
        return []
    return sorted(d.name[5:] for d in root.iterdir()
                  if d.is_dir() and d.name.startswith("date="))


def _trade_days_up_to(end: _date) -> list[_date]:
    """返回 (end-回看窗口, end] 内 A 股交易日（从沪深300 日线索引推导）。"""
    src = MootdxSource()
    start = end - _date(1970, 1, 1)  # placeholder, overwritten below
    from datetime import timedelta as _td
    start = end - _td(days=_DAILY_BACKFILL_LIMIT_DAYS)
    try:
        df = src.get_daily("000300.XSHG", start.strftime("%Y%m%d"),
                           end.strftime("%Y%m%d"))
        if df is not None and not df.empty:
            days = sorted(d.date() for d in df.index)
            return [d for d in days if d <= end]
    except Exception as e:
        logger.warning("mootdx_service: 交易日历获取失败: %s", e)
    # 兜底：工作日近似
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += _td(days=1)
    return days


def _trade_days_in_range(start: _date, end: _date) -> list[_date]:
    """返回 [start, end] 内 A 股交易日（从沪深300 日线推导，全区间不截断）。

    与 ``_trade_days_up_to``（90 天窗口）不同，本函数支持 4/1 至今的全区间
    扫描。取数失败回退工作日近似（不阻断检测）。
    """
    src = MootdxSource()
    try:
        df = src.get_daily("000300.XSHG", start.strftime("%Y%m%d"),
                           end.strftime("%Y%m%d"))
        if df is not None and not df.empty:
            return sorted(d.date() for d in df.index if d.date() <= end)
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 交易日历获取失败: %s", e)
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += _dt.timedelta(days=1)
    return days


def _missing_days_in(calendar: list[_date], root: Path) -> list[_date]:
    """calendar 中不在 root 的 date= 分区里的日期（中间洞也检）。"""
    existing = set(_partition_dates(root))
    return [d for d in calendar if d.isoformat() not in existing]


def scan_missing_partitions(start: _date | None = None) -> dict[str, list[_date]]:
    """分区级缺失扫描：4/1（或 start）至今，6 类数据按交易日历逐日比对。

    检测「交易日历上有、但分区目录无 date= 分区」的日期，含中间洞。
    仅分区级（分区存在即视为该日已覆盖），不逐 symbol 校验。

    对 ETF 日线额外做内容级校验：``_incomplete_etf_daily_days()`` 能把
    "目录存在但内容残缺"（如只剩 1 只）的日子也归为缺失，触发重写，防
    残帧永久污染（见该函数 docstring）。

    额外返回 ``etf_universe_segments``（list[str]）：ETF 宇宙快照缺失的
    权威代码段（如 501/161）。分区覆盖率 vs 残缺宇宙永远测不出"快照缺段"，
    必须用权威段结构做基线（见 ``_etf_universe_segment_missing``）。
    """
    today = _date.today()
    calendar = _trade_days_in_range(start or STOCK_MINUTE_START, today)
    from app.services.etf_nav_service import _missing_etf_nav_days as _missing_nav
    missing_etf_daily = set(_missing_days_in(calendar, ETF_DAILY_ROOT))
    missing_etf_daily |= set(_incomplete_etf_daily_days())
    # 盘中半程快照自愈（08-11 案例）：昨日/历史日分区 mtime 早于自身日期
    # 15:00 即判残缺重写。00:00 巡检跨天也能识别（旧实现只查今天）。
    missing_etf_daily |= set(_stale_daily_days(ETF_DAILY_ROOT))
    missing_stock_daily = set(_missing_days_in(calendar, STOCK_DAILY_ROOT))
    missing_stock_daily |= set(_stale_daily_days(STOCK_DAILY_ROOT))
    missing_stock_minute = set(_missing_days_in(calendar, STOCK_MINUTE_ROOT))
    missing_stock_minute |= set(_incomplete_stock_minute_days())
    seg_missing = _safe_universe_segment_missing()
    if seg_missing:
        logger.warning("mootdx_service: ETF 宇宙快照缺代码段 %s，"
                       "对应标的日线/分钟永不回源，请重建快照",
                       seg_missing)
    return {
        "kline_daily":       sorted(missing_stock_daily),
        "kline_etf_daily":   sorted(missing_etf_daily),
        "kline_index_daily": _missing_days_in(calendar, INDEX_DAILY_ROOT),
        "kline_etf_minute":  _missing_days_in(calendar, ETF_MINUTE_ROOT),
        "kline_minute":      sorted(missing_stock_minute),
        "etf_nav":           _missing_nav(),
        "etf_universe_segments": sorted(seg_missing),
    }


MARKET_CLOSE_TIME = _dt.time(15, 0)  # 当日日线/分钟视为"可回源"的时间下限


def _market_closed(now: _dt.datetime | None = None) -> bool:
    """当前是否已收盘（≥15:00，含盘后）。盘中视为未收盘。"""
    now = now or _dt.datetime.now()
    return now.time() >= MARKET_CLOSE_TIME


def _missing_minute_days(now: _dt.datetime | None = None) -> list[_date]:
    """找出分区缺失的 ETF 分钟交易日（最新分区日期 → 今天）。

    盘中（<15:00）不把"今天"当作缺失日：当日分钟尚未走完，回源只会拿到
    半日数据，写入会污染当天分区（与日线同一问题）。收盘后才把今天算缺失。
    """
    existing = _partition_dates(ETF_MINUTE_ROOT)
    if not existing:
        return []
    latest = _date.fromisoformat(existing[-1])
    now = now or _dt.datetime.now()
    today = now.date()
    if latest >= today and not _market_closed(now):
        return []
    return [d for d in _trade_days_up_to(today) if latest < d <= today]


def _missing_daily_days(root: Path, now: _dt.datetime | None = None) -> list[_date]:
    """找出某日线分区缺失的交易日。

    盘中（<15:00）不把"今天"当作缺失日：当日日线尚未收全，回源只能拿到
    盘中半日快照（如 08-05 10:36 写入的坏分区），且写入后 ``latest==today``
    会让后续回源永久跳过修正。收盘后才把今天算缺失（若今日分区为盘中快照
    则一并重写，见 ``_stale_today_daily_days``），由 backfill 补全。

    在交易日历窗口内检测**所有**缺失日，含 ``latest`` 之前的中间洞：路径
    bug / 回源中断曾让某几日永久搁浅，若只查 ``latest < d`` 则 latest 跳过
    后旧洞永远漏检（08-04/05 案例）。

    中间洞检测限定在**最近 ``_DAILY_BACKFILL_LIMIT_DAYS`` 个交易日**内，
    而不是整个交易日历：``_trade_days_up_to`` 可能返回远超 90 天的全量
    历史（mootdx get_daily 忽略 start 参数），而分区在数据源覆盖起点前
    的"合法空缺"（如股票日线 2023-07~2025-07 空缺）不该被当作缺失重拉。
    窗口内的缺失即真实洞，latest 之后的部分照常补。
    """
    existing = _partition_dates(root)
    if not existing:
        return []
    now = now or _dt.datetime.now()
    today = now.date()
    # 盘中（<15:00）绝不把「今天」当缺失日回源：当日日线尚未收全，回源只会
    # 拉到盘中半日数据，写入即污染分区（08-11 12:50 重启时正是如此产生坏帧）。
    # 无论今日分区是否存在，盘中都不回源今天；中间洞（latest 之前）照常检。
    if not _market_closed(now):
        if existing[-1] >= today.isoformat():
            return []
        calendar = _trade_days_up_to(today)
        recent = calendar[-_DAILY_BACKFILL_LIMIT_DAYS:]
        days = _missing_days_in(recent, root)
        return [d for d in days if d < today]
    calendar = _trade_days_up_to(today)
    recent = calendar[-_DAILY_BACKFILL_LIMIT_DAYS:]
    days = _missing_days_in(recent, root)
    # 收盘后：最近分区中「早于其自身日期 15:00 写入」的盘中快照（含今日与
    # 历史日）都要强制重写为收盘完整数据（否则盘中坏分区会永久残留）。
    stale = _stale_daily_days(root, now)
    if stale:
        return sorted(set(days) | set(stale))
    return days


def _etf_universe_segment_missing(codes: list[str]) -> list[str]:
    """返回 ETF 宇宙中缺失的权威代码段（如 ``501``/``161``）。

    背景：旧内容校验只比「分区覆盖率 vs 当前宇宙」，但宇宙快照本身可能残缺
    （如服务器快照缺整个 501/161 段），此时分区覆盖率恒高、永不告警。本函数
    以 ``_ETF_UNIVERSE_EXPECTED_SEGMENTS``（聚宽名单段结构）为权威基线，
    宇宙里完全没有某段的任何代码即判该段缺失。

    返回缺失段列表（空 = 完整）。调用方应在此之前拦截空宇宙。
    """
    if not codes:
        return list(_ETF_UNIVERSE_EXPECTED_SEGMENTS)
    have = {c.split(".", 1)[0][:3] for c in codes if "." in c}
    return [seg for seg in _ETF_UNIVERSE_EXPECTED_SEGMENTS if seg not in have]


def _safe_universe_segment_missing() -> list[str]:
    """读取宇宙并返回缺失段；宇宙读取失败时降级为空（不阻断巡检）。"""
    try:
        codes = _etf_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: ETF 宇宙读取失败，跳过段校验", exc_info=True)
        return []
    if not codes:
        return []
    return _etf_universe_segment_missing(codes)


def _incomplete_etf_daily_days(recent: int | None = None) -> list[_date]:
    """返回 ETF 日线中**内容残缺**的分区日期（符号数 << ETF 宇宙）。

    背景：宽缺口判定（``_missing_daily_days``/``_partition_dates``）只检查
    分区目录是否存在，一个**只有 1 只 ETF **的分区与 1600 只的完整分区
    被视为等价——一旦某天曾以残缺状态落盘（回源中断、宇宙快照零星等），
    增量追溯永不重写，残帧永久污染净值曲线。

    这里做内容级校验：对最近 ``recent``（默认 30）个分区，读 symbol 列与
    ETF 宇宙比对，覆盖率 < ``_ETF_DAILY_MIN_COVERAGE``（默认 0.5）即判残缺。
    宇宙为空时跳过（无基线可比）；覆盖率 >= 阈值的正常分区不误报。
    """
    existing = _partition_dates(ETF_DAILY_ROOT)
    if not existing:
        return []
    try:
        codes = _etf_universe()
    except Exception:
        logger.warning("mootdx_service: ETF 宇宙读取失败，跳过内容校验", exc_info=True)
        return []
    if not codes:
        return []

    recent = _ETF_DAILY_RECENT_LIMIT if recent is None else recent
    target = set(_to_tf_symbol(c) for c in codes)
    out: list[_date] = []
    for ds in existing[-recent:]:
        pdir = ETF_DAILY_ROOT / f"date={ds}"
        parts = sorted(pdir.glob("*.parquet"))
        if not parts:
            continue
        syms: set[str] = set()
        for p in parts:
            try:
                df = pl.read_parquet(p, columns=None)  # ETF 分区无 date 列，读全列取 symbol
                syms |= set(df["symbol"].to_list())
            except Exception:
                continue
        coverage = len(syms & target) / len(target)
        if coverage < _ETF_DAILY_MIN_COVERAGE:
            out.append(_dt.date.fromisoformat(ds))
    logger.info("mootdx_service: ETF 日线内容校验 %d/%d 分区, 残缺 %d: %s",
                len(existing[-recent:]), len(existing), len(out),
                [d.isoformat() for d in out])
    return out


def _incomplete_stock_minute_days(recent: int | None = None) -> list[_date]:
    """返回股票分钟 ``kline_minute`` 中**内容残缺**的分区日期（symbol 覆盖率 << 宇宙）。

    背景：宽缺口判定（``_missing_days_in``/``_partition_dates``）只检查分区
    目录是否存在，一个只有 1 只股票的分区与完整分区被视为等价——一旦某天曾
    以残缺状态落盘（回源中断、全市场失败如 pytdx 缺失事故 5226 只全失败），
    增量追溯永不重写，最新交易日分钟数据永久缺失。

    这里做内容级校验：对最近 ``recent``（默认由 ``_STOCK_MINUTE_RECENT_LIMIT``
    控制）个分区，读 symbol 列与股票宇宙（``_stock_universe``，.SH/.SZ）比对，
    覆盖率 < ``_STOCK_MINUTE_MIN_COVERAGE``（默认 0.5）即判残缺。宇宙为空时
    跳过（无基线可比）；覆盖率 >= 阈值的正常分区不误报。

    盘中（<15:00）跳过当日分区：当日分钟本就走了一半（未收盘），覆盖率天然
    偏低，误判会触发 ``sync_stock_minute_range`` 全市场重拉（~2h 浪费），
    且写回的是半程数据。收盘后才把当日纳入残缺校验（对齐 ``_missing_minute_days``
    口径）。
    """
    existing = _partition_dates(STOCK_MINUTE_ROOT)
    if not existing:
        return []
    try:
        codes = _stock_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 股票宇宙读取失败，跳过分钟内容校验", exc_info=True)
        return []
    if not codes:
        return []

    today = _date.today()
    recent = _STOCK_MINUTE_RECENT_LIMIT if recent is None else recent
    target = set(codes)
    out: list[_date] = []
    for ds in existing[-recent:]:
        d = _dt.date.fromisoformat(ds)
        if d == today and not _market_closed():
            logger.info("mootdx_service: 当日 %s 盘中未收盘，跳过残缺校验", ds)
            continue
        pdir = STOCK_MINUTE_ROOT / f"date={ds}"
        parts = sorted(pdir.glob("*.parquet"))
        if not parts:
            continue
        syms: set[str] = set()
        for p in parts:
            try:
                df = pl.read_parquet(p, columns=["symbol"])
                syms |= set(df["symbol"].to_list())
            except Exception:  # noqa: BLE001
                continue
        coverage = len(syms & target) / len(target)
        if coverage < _STOCK_MINUTE_MIN_COVERAGE:
            out.append(_dt.date.fromisoformat(ds))
    logger.info("mootdx_service: 股票分钟内容校验 %d/%d 分区, 残缺 %d: %s",
                len(existing[-recent:]), len(existing), len(out),
                [d.isoformat() for d in out])
    return out


def _stock_universe() -> list[str]:
    """返回全市场 A 股 symbol 列表（.SH/.SZ，优先 instruments parquet）。"""
    inst_path = DATA_ROOT / "instruments" / "instruments.parquet"
    if inst_path.exists():
        try:
            df = pl.read_parquet(inst_path, columns=["symbol"])
            syms = df["symbol"].to_list()
            if syms:
                return sorted(syms)
        except Exception as e:
            logger.warning("mootdx_service: instruments 读取失败: %s", e)
    # 兜底：从已有日线分区收集
    lf = pl.scan_parquet(str(STOCK_DAILY_ROOT / "**" / "*.parquet"),
                         hive_partitioning=True)
    return sorted(lf.select("symbol").unique().collect()["symbol"].to_list())


def _jq_to_tf_symbol(sym: str) -> str:
    """分区 symbol (.SH/.SZ) -> mootdx 6位纯代码。"""
    return sym.split(".")[0]


def _guarded_get_daily(src: MootdxSource, sym: str, start: str, end: str,
                       timeout: float = 20.0) -> pd.DataFrame | None:
    """带墙钟超时的 mootdx 日线取数，防长批回源时单只卡死整批。

    mootdx 底层 socket 在服务器劣化时可能永久阻塞（`_with_server_retry` 的
    线程 join 有 10s 超时，但 `_probe`/`_make_client` 等路径仍可能挂起）。
    这里在调用外再包一层线程守护，超时则弃帧不阻断整批。
    """
    import threading as _th
    box: dict = {}

    def _run() -> None:
        try:
            box["df"] = src.get_daily(sym, start, end)
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    t = _th.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        # 卡死：弃帧并强制重建连接，避免后续请求继续用坏 socket
        try:
            src._client = None
            src._server_idx = -1
        except Exception:
            pass
        logger.warning("mootdx_service: %s 日线超时(%ss)，弃帧重建连接", sym, timeout)
        return None
    if "err" in box:
        return None
    return box.get("df")


def _guarded_get_minute(src: MootdxSource, sym: str, max_bars: int = 40000,
                        timeout: float = 30.0) -> pd.DataFrame | None:
    """带墙钟超时的 mootdx 分钟取数，防长批回源时单只卡死整批。

    ``get_minute`` 分页拉取（单只最多 50 页），正常 ~2s、慢标的 ~15s；服务器
    劣化时底层 socket 可能永久挂起。这里外包线程守护，超时**抛异常**让调用方
    重建 MootdxSource（彻底刷新连接状态，避免坏 socket/坏 server 索引残留）。
    """
    import threading as _th
    box: dict = {}

    def _run() -> None:
        try:
            box["df"] = src.get_minute(sym, max_bars=max_bars)
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    t = _th.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"{sym} 分钟拉取超时({timeout}s)")
    if "err" in box:
        raise box["err"]
    return box.get("df")


def sync_daily(day: _date) -> dict:
    """回源指定交易日全市场日线（股票 kline_daily + ETF kline_etf_daily）。

    逐只标的用 mootdx ``get_daily`` 拉最近日线，取 ``day`` 那根写分区：
    股票 volume 换手（mootdx 股 ÷100），ETF volume 保持股。返回统计。
    """
    # 北交所（920xxx.BJ）mootdx 通达信接口无数据（每只轮换全服务器 ~8-11s 超时），
    # 全量回源时跳过，避免 331 只累积 ~50 分钟纯失败。
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    etfs = [_to_tf_symbol(c) for c in _etf_universe()]
    src = MootdxSource()
    day_str = day.strftime("%Y%m%d")
    # 跳过上市日晚于目标日的标的（新股在该日前无数据）；上市日期占位
    # （1970-01-01 = 退市/异常）的标的 4/1 前已无交易，一并跳过免超时。
    # 注意：只对股票做该过滤。ETF 宇宙来自 etf_universe_snapshot（无退市占位），
    # 且 _listing_date_map 读的是股票 instruments 表，对 ETF 过滤会把全部 ETF
    # 判为 1970 占位退市 → daily_written.etf 恒 0，ETF 日线永不落盘。
    listing = _listing_date_map()
    if listing:
        def _active(sym: str) -> bool:
            ld = listing.get(sym, _date(1970, 1, 1))
            return ld > _date(1970, 1, 1) and ld <= day
        stocks = [s for s in stocks if _active(s)]
    written = {"stock": 0, "etf": 0}
    frames_stock: list[pl.DataFrame] = []
    frames_etf: list[pl.DataFrame] = []

    def _fetch(syms: list[str]) -> pl.DataFrame | None:
        out_frames = []
        for i, sym in enumerate(syms):
            _throttle_backfill(i)
            try:
                df = _guarded_get_daily(src, sym, day_str, day_str)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            # 只保留目标日那根（索引含 15:00 时间戳）
            hit = df[[x.date() == day for x in df.index]]
            if hit.empty:
                continue
            row = hit.iloc[-1]
            out_frames.append(pl.DataFrame({
                "symbol": [sym],
                "date": [day],
                "open": [float(row["open"])],
                "high": [float(row["high"])],
                "low": [float(row["low"])],
                "close": [float(row["close"])],
                "volume": [float(row["volume"])],
                "amount": [float(row["amount"])],
            }))
            # 周期性重建连接：避免服务器轮换状态劣化导致长批卡死
            if (i + 1) % 500 == 0:
                try:
                    src._client = None
                    src._server_idx = -1
                except Exception:
                    pass
        if not out_frames:
            return None
        return pl.concat(out_frames)

    sdf = _fetch(stocks)
    if sdf is not None:
        sdf = sdf.with_columns((pl.col("volume") / 100.0).alias("volume"))
        frames_stock.append(sdf)
        written["stock"] = sdf.height
    edf = _fetch(etfs)
    if edf is not None:
        frames_etf.append(edf)
        written["etf"] = edf.height
    # 防御：回源结果为空不能静默——否则分区缺口无声累积（曾因过滤 bug 让
    # ETF 日线数月未落盘而不自知）。全市场全失败通常意味着数据源/过滤异常。
    if frames_stock and not frames_etf:
        logger.warning("mootdx_service: 日线回源 %s 股票 %d 只但 ETF 全部失败，"
                       "请检查 ETF 宇宙/过滤逻辑", day, written["stock"])
    if not frames_stock and not frames_etf:
        logger.warning("mootdx_service: 日线回源 %s 股票与 ETF 全部失败", day)

    if frames_stock:
        _write_daily_partition(pl.concat(frames_stock), STOCK_DAILY_ROOT)
    if frames_etf:
        _write_daily_partition(pl.concat(frames_etf), ETF_DAILY_ROOT)
    logger.info("mootdx_service: 日线回源 %s 完成: %s", day, written)
    return written


def _write_daily_partition(df: pl.DataFrame, root: Path) -> None:
    """按 date 分区原子写日线（读旧→concat→unique→tmp→rename）。

    兼容两类既有格式：
    - 股票日线（kline_daily）：文件内带 ``date`` 列；
    - ETF 日线（kline_etf_daily）：文件内**无** date 列，日期由 hive 分区
      目录名隐含。若既有分区无 date 列，合并前去掉新帧的 date 列，保持
      与既有格式一致（否则 concat 列数不一致报 ShapeError）。
    """
    ds = df["date"][0].isoformat() if hasattr(df["date"][0], "isoformat") else str(df["date"][0])
    pdir = root / f"date={ds}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    tmp = pdir / "part.tmp"
    if part.exists():
        old = pl.read_parquet(part)
        if "date" not in old.columns:
            df = df.drop("date")
            merged = pl.concat([old, df]).unique(
                subset=["symbol"], keep="last").sort(["symbol"])
        else:
            merged = pl.concat([old, df]).unique(
                subset=["symbol", "date"], keep="last").sort(["symbol", "date"])
    else:
        merged = df.sort(["symbol", "date"])
    merged.write_parquet(tmp)
    tmp.rename(part)


def _index_universe() -> list[str]:
    """返回指数日线回源 symbol 列表（.SH/.SZ，优先 instruments_index 表）。

    指数宇宙：``data/instruments_index/instruments_index.parquet`` 的 symbol 列。
    缺失/读取失败时兜底模拟盘固定 4 只（000300/000510/399006/399101），确保
    弱市判断/基准数据始终可用。
    """
    inst_path = DATA_ROOT / "instruments_index" / "instruments_index.parquet"
    if inst_path.exists():
        try:
            df = pl.read_parquet(inst_path, columns=["symbol"])
            syms = df["symbol"].to_list()
            if syms:
                return sorted(syms)
        except Exception:  # noqa: BLE001
            logger.warning("mootdx_service: instruments_index 读取失败, 用兜底指数")
    return ["000300.SH", "000510.SH", "399006.SZ", "399101.SZ"]


def sync_index_daily(day: _date) -> dict:
    """回源指定交易日全市场指数日线到 ``kline_index_daily`` 分区。

    逐个指数用 mootdx ``get_daily``（指数自动走 index_bars）拉最近日线，
    取 ``day`` 那根写按日分区。跳过北交所（899xxx.BJ mootdx 无数据）。
    返回 {"written": 写入指数数, "symbols": 尝试数}。
    """
    indices = [s for s in _index_universe() if not s.endswith(".BJ")]
    src = MootdxSource()
    day_str = day.strftime("%Y%m%d")
    frames: list[pl.DataFrame] = []
    for i, sym in enumerate(indices):
        _throttle_backfill(i)
        try:
            df = _guarded_get_daily(src, sym, day_str, day_str)
            if df is None or df.empty:
                continue
            hit = df[[x.date() == day for x in df.index]]
            if hit.empty:
                continue
            row = hit.iloc[-1]
            frames.append(pl.DataFrame({
                "symbol": [sym],
                "date": [day],
                "open": [float(row["open"])],
                "high": [float(row["high"])],
                "low": [float(row["low"])],
                "close": [float(row["close"])],
                "volume": [float(row["volume"])],
                "amount": [float(row["amount"])],
            }))
        except Exception:  # noqa: BLE001
            continue
        if (i + 1) % 500 == 0:
            try:
                src._client = None
                src._server_idx = -1
            except Exception:  # noqa: BLE001
                pass
    if not frames:
        logger.warning("mootdx_service: 指数日线回源 %s 全部失败（%d 只尝试）",
                       day, len(indices))
        return {"written": 0, "symbols": len(indices)}
    _write_daily_partition(pl.concat(frames), INDEX_DAILY_ROOT)
    logger.info("mootdx_service: 指数日线回源 %s 完成: %d 只", day, len(frames))
    return {"written": len(frames), "symbols": len(indices)}


def _missing_index_daily_days() -> list[_date]:
    """找出 ``kline_index_daily`` 分区缺失的交易日。"""
    return _missing_daily_days(INDEX_DAILY_ROOT)


def _stale_daily_days(root: Path, now: _dt.datetime | None = None,
                      recent: int | None = None) -> list[_date]:
    """收盘后找出最近日线分区中「早于该分区自身日期收盘」写入的盘中快照。

    场景：盘中某次 backfill 把半日数据当完整日线写入（mtime < 该日 15:00）。
    旧实现 ``_stale_today_daily_days`` 只查"今天"——昨天 12:50 写入的半程
    快照一旦跨天就永远漏检（08-11 案例：15:35 cron 缺席，次日无人重写）。
    本函数按分区**自身的日期**判定：part.parquet 的 mtime 早于该日期 15:00
    即盘中写入，无论"今天"是几号都能识别，跨天也能自愈。
    """
    now = now or _dt.datetime.now()
    existing = _partition_dates(root)
    if not existing:
        return []
    recent = _DAILY_BACKFILL_LIMIT_DAYS if recent is None else recent
    out: list[_date] = []
    for ds in existing[-recent:]:
        d = _dt.date.fromisoformat(ds)
        pdir = root / f"date={ds}"
        part = pdir / "part.parquet"
        if not part.exists():
            continue
        try:
            mt = _dt.datetime.fromtimestamp(part.stat().st_mtime)
        except OSError:
            continue
        # 盘中快照：写入时刻早于该分区日期的 15:00 收盘
        if mt < _dt.datetime.combine(d, MARKET_CLOSE_TIME):
            out.append(d)
    return out


def _stale_today_daily_days(root: Path, now: _dt.datetime | None = None) -> list[_date]:
    """收盘后发现「今天」分区存在但写入时间早于收盘（盘中快照）→ 需重写。

    保留兼容旧测试/调用；通用判定见 ``_stale_daily_days``（覆盖任意历史日）。
    """
    now = now or _dt.datetime.now()
    today = now.date()
    return [today] if today in _stale_daily_days(root, now) else []


def _adj_factor_stale() -> bool:
    """判断 ETF 前复权因子表是否落后于 ETF 日线最新交易日。

    因子表 ``all.parquet`` 的 ``max(trade_date)`` 落后于 ``kline_etf_daily``
    最新分区日期（或有新增交易日）即视为 stale，需重新 ``sync_adj_factor``。
    文件不存在视为 stale（首次部署）。
    """
    if not ADJ_FACTOR_PATH.exists():
        return True
    try:
        df = pl.read_parquet(ADJ_FACTOR_PATH, columns=["trade_date"])
        if df.is_empty():
            return True
        factor_latest = df["trade_date"].max()
        if not isinstance(factor_latest, _date):
            return True
    except Exception:  # noqa: BLE001
        return True
    etf_days = _partition_dates(ETF_DAILY_ROOT)
    if not etf_days:
        # 无 ETF 日线分区：以因子表自身为基准，不误判（有分区时以下逻辑生效）
        return factor_latest < _date.today()
    etf_latest = _date.fromisoformat(etf_days[-1])
    return factor_latest < etf_latest


def _notify_missing(missing: dict) -> None:
    """空分区/缺口/宇宙缺段时打 WARNING 日志并尝试钉钉站内信通知用户。fire-and-forget。"""
    lines = []
    for name, st in missing.items():
        latest = st.get("latest") or "无"
        line = f"- {name}: 最新 {latest}（empty={st.get('empty')}, missing={st.get('missing')}）"
        seg = st.get("segment_missing")
        if seg:
            line += f" [宇宙缺代码段: {seg}]"
        lines.append(line)
    msg = "mootdx 启动回源发现以下数据集缺失/缺口:\n" + "\n".join(lines)
    logger.warning("mootdx_service: %s", msg)
    try:
        from app.quant import db as qdb
        webhook = qdb.get_quant_setting("dingtalk_webhook_url") or ""
        secret = qdb.get_quant_setting("dingtalk_secret") or ""
        if webhook:
            from app.quant.notify import send_dingtalk
            send_dingtalk(webhook, secret, "模拟盘数据回源缺口", msg)
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 钉钉缺口通知失败（忽略）")


def backfill_to_now() -> dict[str, Any]:
    """启动回源：补齐到当前时间缺失的全部数据集（幂等）。

    覆盖：ETF 分钟 + 全市场日线（股票/ETF）+ 指数日线 + 股票分钟一批 +
    ETF 前复权因子表（stale 时）。缺日期分区回溯补窗口；完全空分区也补窗口
    并标记 missing + 钉钉告警。失败标的跳过不阻断。
    返回结果含 index_daily_days/index_daily_written/adj_factor/missing。
    """
    result: dict[str, Any] = {
        "minute_days": [], "minute_rows": 0,
        "daily_days": [], "daily_written": {},
        "index_daily_days": [], "index_daily_written": {},
        "adj_factor": None,
        "stock_minute_rows": 0, "stock_minute_days": [], "etf_nav_days": [], "errors": [],
    }

    from app.services import etf_nav_service
    stocks_daily      = _partition_dates(STOCK_DAILY_ROOT)
    etf_daily_days    = _partition_dates(ETF_DAILY_ROOT)
    index_daily_days  = _partition_dates(INDEX_DAILY_ROOT)
    etf_minute_days   = _partition_dates(ETF_MINUTE_ROOT)
    etf_nav_days      = etf_nav_service._partition_dates()
    missing_nav_days  = etf_nav_service._missing_etf_nav_days()

    result["missing"] = {
        "kline_etf_minute":   {"latest": etf_minute_days[-1] if etf_minute_days else None,
                               "empty": not etf_minute_days, "missing": bool(_missing_minute_days())},
        "kline_daily":        {"latest": stocks_daily[-1] if stocks_daily else None,
                               "empty": not stocks_daily, "missing": bool(_missing_daily_days(STOCK_DAILY_ROOT))},
        "kline_etf_daily":    {"latest": etf_daily_days[-1] if etf_daily_days else None,
                               "empty": not etf_daily_days,
                               "missing": bool(_missing_daily_days(ETF_DAILY_ROOT)
                                               or _incomplete_etf_daily_days()),
                               "segment_missing": _safe_universe_segment_missing()},
        "kline_index_daily":  {"latest": index_daily_days[-1] if index_daily_days else None,
                               "empty": not index_daily_days, "missing": bool(_missing_index_daily_days())},
        "kline_minute":       {"latest": None, "empty": not _partition_dates(STOCK_MINUTE_ROOT),
                               "missing": bool(_incomplete_stock_minute_days())},
        "adj_factor_etf":     {"latest": None, "empty": not ADJ_FACTOR_PATH.exists(), "missing": _adj_factor_stale()},
        "etf_nav":            {"latest": etf_nav_days[-1] if etf_nav_days else None,
                               "empty": not etf_nav_days, "missing": bool(missing_nav_days)},
    }

    # 1. ETF 分钟
    for day in _missing_minute_days():
        try:
            n = sync_etf_minute(day)
            result["minute_days"].append(str(day))
            result["minute_rows"] += n
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 分钟回源 %s 失败: %s", day, e)
            result["errors"].append(f"minute {day}: {e}")

    # 2. 日线（股票 + ETF）——统一用一个交易日历；空时补最近窗口
    today = _date.today()
    daily_days = sorted(set(_missing_daily_days(STOCK_DAILY_ROOT))
                        | set(_missing_daily_days(ETF_DAILY_ROOT))
                        | set(_incomplete_etf_daily_days()))
    # 股票日线根为空时的兜底种子窗口；残缺 ETF 日（内容校验）必须保留，
    # 否则会被空窗分支整体覆盖而漏补。
    if _missing_daily_days(STOCK_DAILY_ROOT) == [] and not stocks_daily:
        seed = set(_trade_days_up_to(today)) - set(_partition_dates(STOCK_DAILY_ROOT))
        daily_days = sorted(seed | set(_incomplete_etf_daily_days()))
    for day in daily_days:
        try:
            w = sync_daily(day)
            result["daily_days"].append(str(day))
            for k, v in w.items():
                result["daily_written"][k] = result["daily_written"].get(k, 0) + v
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 日线回源 %s 失败: %s", day, e)
            result["errors"].append(f"daily {day}: {e}")

    # 2b. 指数日线（新增）——空时补最近窗口
    idx_days = sorted(_missing_index_daily_days())
    if not idx_days and not index_daily_days:
        idx_days = sorted(set(_trade_days_up_to(today)) - set(_partition_dates(INDEX_DAILY_ROOT)))
    for day in idx_days:
        try:
            w = sync_index_daily(day)
            result["index_daily_days"].append(str(day))
            for k, v in w.items():
                result["index_daily_written"][k] = result["index_daily_written"].get(k, 0) + v
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 指数日线回源 %s 失败: %s", day, e)
            result["errors"].append(f"index_daily {day}: {e}")

    # 2c. ETF 因子表（新增）——stale 才跑
    if result["missing"]["adj_factor_etf"]["missing"] or not ADJ_FACTOR_PATH.exists():
        try:
            result["adj_factor"] = sync_adj_factor()
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 因子表回源失败: %s", e)
            result["errors"].append(f"adj_factor: {e}")

    # 3. 股票分钟：先修复内容残缺分区（range 全量，走所有缺失日前的分支），
    #    再跑增量慢跑（每次一批，resume 跳过已覆盖，多轮自动补齐）
    incomplete_minute = _incomplete_stock_minute_days()
    if incomplete_minute:
        try:
            n = sync_stock_minute_range(incomplete_minute)
            result["stock_minute_rows"] = result.get("stock_minute_rows", 0) + n
            result["stock_minute_days"] = [d.isoformat() for d in incomplete_minute]
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 股票分钟残缺分区重写失败 %s: %s",
                           incomplete_minute, e)
            result["errors"].append(f"stock_minute_range {incomplete_minute}: {e}")
    try:
        n = sync_stock_minute(limit=STOCK_MINUTE_BATCH_LIMIT)
        result["stock_minute_rows"] = result.get("stock_minute_rows", 0) + n
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 股票分钟回源失败: %s", e)
        result["errors"].append(f"stock_minute: {e}")

    # 3b. ETF 单位净值（akshare，只补最新缺失交易日；盘中未收盘 → 目标前一交易日）
    for day in missing_nav_days:
        try:
            etf_nav_service.sync_etf_nav(day)
            result["etf_nav_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 净值回源 %s 失败: %s", day, e)
            result["errors"].append(f"etf_nav {day}: {e}")

    # 4. 缺口告警（日志 + 钉钉）
    if any((st["missing"] or st["empty"] or st.get("segment_missing"))
           for st in result["missing"].values()):
        _notify_missing(result["missing"])

    logger.info("mootdx_service: 启动回源完成 %s", result)
    return result
