# backend/app/services/stockdata/scheduler.py
"""自治调度：启动 backfill + 15:35 收盘批量同步 + 00:00 清空当日分钟内存（线程）。

无主动盘中全市场轮询——实时分钟只在客户端请求时按需回源（见 sources.get_realtime_snapshot）。"""
from __future__ import annotations

import contextlib
import datetime as _dt
import logging
import threading
import time


def _sync_lock() -> threading.Lock:
    """15:35 cron / 00:00 巡检 / 启动 backfill 共用锁（惰性解析）。

    锁定义在 mootdx_service（:data:`_SYNC_LOCK`）。调用时取模块属性而非
    import 期绑定：测试里 ``importlib.reload(mootdx_service)`` 会重建锁对象，
    惰性解析保证 scheduler 始终与当前模块实例同锁。
    """
    from app.services import mootdx_service

    return mootdx_service._SYNC_LOCK

logger = logging.getLogger("app.services.stockdata.scheduler")

_scheduler_state: dict = {"last_backfill": None, "last_sync": None, "sync_job": None}
_lock = threading.Lock()
_stop = threading.Event()
_threads: list[threading.Thread] = []

# 当前正在执行的后台任务集合（供 get_status 查询"正在干什么"）
_active_tasks: set[str] = set()
_PROCESS_STARTED = _dt.datetime.now().isoformat()


def _mark_active(name: str) -> None:
    with _lock:
        _active_tasks.add(name)


def _mark_idle(name: str) -> None:
    with _lock:
        _active_tasks.discard(name)


def _trim_memory() -> None:
    """gc + glibc malloc_trim：把已释放堆归还 OS，压回回源/大查询后的高水位。

    polars/pandas 大帧释放后 glibc 不主动还页，RSS 停在峰值（7G 级小内存
    机器会挤压 swap）；trim 只还空闲页，不影响活跃分配。
    """
    import ctypes
    import gc

    gc.collect()
    with contextlib.suppress(Exception):
        ctypes.CDLL("libc.so.6").malloc_trim(0)


def _idle_trim_loop(interval: float = 600.0) -> None:
    """周期巡检：无活动任务时执行 :func:`_trim_memory`。"""
    while not _stop.is_set():
        if _stop.wait(interval):
            break
        with _lock:
            busy = bool(_active_tasks)
        if busy:
            continue
        _trim_memory()


def _json_safe(value):
    """递归把 date/datetime 转字符串，其余保持（供 msgpack/json 序列化）。"""
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def get_status() -> dict:
    """返回 scheduler 状态快照（最近任务结果 + 当前正在执行的任务），JSON 安全。

    供 status handler 经 TCP 回传主后端，前端用于展示"服务在干什么 / 有哪些待办"。
    """
    with _lock:
        state = dict(_scheduler_state)
        active = sorted(_active_tasks)
    state["active_tasks"] = active
    state["ts"] = _dt.datetime.now().isoformat()
    state["process_started"] = _PROCESS_STARTED
    return _json_safe(state)


def _backfill_loop():
    _mark_active("backfill")
    try:
        from app.services import mootdx_service
        res = mootdx_service.backfill_to_now()
        with _lock:
            _scheduler_state["last_backfill"] = str(_dt.datetime.now())
            _scheduler_state["backfill_result"] = res
        logger.info("stockdata startup backfill done: %s", res)
        # 季频财务（gpcw）幂等回源：已有分区秒级跳过，新季度才下载
        from app.services import tdx_financials
        fin = tdx_financials.sync_financials()
        with _lock:
            _scheduler_state["financials_sync"] = fin
        logger.info("stockdata financials sync done: %s", fin)
    except Exception:  # noqa: BLE001
        logger.exception("stockdata startup backfill failed")
    finally:
        _mark_idle("backfill")
        _trim_memory()


def _run_sync(full_stock_minute: bool = False):
    """收盘后同步（15:35 cron 传 full_stock_minute=True）。

    ``full_stock_minute``：收盘后整批把当天全市场股票分钟拉全（~2h），否则
    增量慢跑（每轮 limit=20）。手动 trigger_sync 保持增量 + 不落日线
    （盘中触发写半程日线会污染分区）。
    """
    with _sync_lock():
        _mark_active("sync")
        try:
            from app.services import mootdx_service
            minutes = mootdx_service.sync_etf_minute()
            adj = mootdx_service.sync_adj_factor()
            daily: dict | None = None
            index_daily: dict | None = None
            if full_stock_minute:
                # 收盘后：当天全市场股票分钟一次拉全（增量慢跑 20 只/轮
                # 每交易日只补 20 只，5200 只需 260 轮，永远追不上当天）。
                stock = mootdx_service.sync_stock_minute(limit=None)
                from app.services import etf_nav_service
                nav = etf_nav_service.sync_etf_nav()
                today = _dt.date.today()
                daily = mootdx_service.sync_daily(today)
                index_daily = mootdx_service.sync_index_daily(today)
            else:
                stock = mootdx_service.sync_stock_minute(
                    limit=mootdx_service.STOCK_MINUTE_BATCH_LIMIT)
                from app.services import etf_nav_service
                nav = etf_nav_service.sync_etf_nav()
            with _lock:
                _scheduler_state["last_sync"] = str(_dt.datetime.now())
                _scheduler_state["sync_result"] = {
                    "minute_rows": minutes, "adj": adj, "stock_minute_rows": stock,
                    "nav_rows": nav, "daily": daily, "index_daily": index_daily}
            logger.info("scheduled mootdx sync done: minute=%s, adj=%s, stock_minute=%s, nav_rows=%s, daily=%s, index_daily=%s",
                        minutes, adj, stock, nav, daily, index_daily)
        except Exception:  # noqa: BLE001
            logger.exception("scheduled mootdx sync failed")
        finally:
            _mark_idle("sync")
            _trim_memory()


def _run_check_day(day: str) -> None:
    """单日检验补齐（后台线程）：解析日期并执行。"""
    with _sync_lock():
        _mark_active("check_day")
        try:
            from app.services import mootdx_service
            d = _dt.date.fromisoformat(day)
            res = mootdx_service.check_and_repair_day(d)
            with _lock:
                _scheduler_state["last_check_day"] = day
                _scheduler_state["check_day_result"] = res
            logger.info("stockdata check_day %s done: %s", day,
                        {k: v["status"] for k, v in res["results"].items()})
        except Exception:  # noqa: BLE001
            logger.exception("stockdata check_day %s failed", day)
        finally:
            _mark_idle("check_day")
            _trim_memory()


def _run_check_full() -> None:
    """全量检验补齐（后台线程）：执行并记录汇总。"""
    with _sync_lock():
        _mark_active("check_full")
        try:
            from app.services import mootdx_service
            res = mootdx_service.check_and_repair_full()
            with _lock:
                _scheduler_state["last_check_full"] = str(_dt.datetime.now())
                _scheduler_state["check_full_result"] = res
            logger.info("stockdata check_full done: %s",
                        {k: len(v) for k, v in (res.get("missing") or {}).items()}
                        if isinstance(res.get("missing"), dict) else res)
        except Exception:  # noqa: BLE001
            logger.exception("stockdata check_full failed")
        finally:
            _mark_idle("check_full")
            _trim_memory()


def _sync_cron_loop():
    """15:35（工作日）触发 _run_sync；非交易日不触发。"""
    while not _stop.is_set():
        now = _dt.datetime.now()
        if (now.weekday() < 5 and now.time() >= _dt.time(15, 35)
                and now.time() < _dt.time(15, 36)):
            with _lock:
                last = _scheduler_state.get("sync_job")
            if last != now.date().isoformat():
                _scheduler_state["sync_job"] = now.date().isoformat()
                threading.Thread(target=_run_sync, kwargs={"full_stock_minute": True},
                                 daemon=True).start()
        time.sleep(30)


def _run_full_scan_once() -> None:
    """00:00 全量缺失巡检 + 补全（单次执行体，与 15:35 用 _sync_lock 串行）。

    进入即标记 ``full_scan_started`` 为今天，完成后再写 ``full_scan_date``。
    watchdog 用 ``full_scan_started`` 判断"今天的巡检是否已启动"——巡检缺席
    （线程未拉起，如 08-09/08-10 连续两晚无完成日志）时能打 WARNING 告警。
    """
    with _lock:
        _scheduler_state["full_scan_started"] = _dt.date.today().isoformat()
    with _sync_lock():
        _mark_active("full_scan")
        try:
            from app.services import mootdx_service
            res = mootdx_service.scan_and_backfill_full()
            with _lock:
                _scheduler_state["last_full_scan"] = str(_dt.datetime.now())
                _scheduler_state["full_scan_result"] = res
                _scheduler_state["full_scan_date"] = _dt.date.today().isoformat()
            logger.info("stockdata midnight full scan done: %s",
                        {k: len(v) for k, v in (res.get("missing") or {}).items()}
                        if isinstance(res.get("missing"), dict) else res)
        except Exception:  # noqa: BLE001
            logger.exception("stockdata midnight full scan failed")
        finally:
            _mark_idle("full_scan")
            _trim_memory()


def _midnight_scan_loop():
    """00:00 触发全量缺失巡检；每日一次，跨日重置。"""
    last_date = None
    while not _stop.is_set():
        now = _dt.datetime.now()
        if (now.time() >= _dt.time(0, 0) and now.time() < _dt.time(0, 1)
                and now.date() != last_date):
            last_date = now.date()
            threading.Thread(target=_run_full_scan_once, daemon=True).start()
        time.sleep(20)


def _full_scan_started_today(state: dict, now: _dt.datetime) -> bool:
    """watchdog 判定：今天的 00:00 巡检是否**已启动**（线程已拉起，含被锁阻塞中）。"""
    return state.get("full_scan_started") == now.date().isoformat()


def _warn_if_full_scan_incomplete(state: dict, now: _dt.datetime) -> None:
    """今日巡检未启动则打 WARNING（供钉钉消费）；已启动静默。

    判定用 ``full_scan_started`` 而非 ``full_scan_date``：scan 线程拉起后
    可能被 ``_sync_lock`` 阻塞（15:35 长任务占用锁），00:05 时尚未完成是
    正常的，不能误告警；只有今天**从未启动**（线程没拉起/巡检缺席，如
    08-09/08-10）才算异常。
    """
    if not _full_scan_started_today(state, now):
        logger.warning(
            "stockdata midnight full scan NOT started today (%s): "
            "loop not firing, thread stuck, or scheduler stopped",
            now.date())


def _full_scan_watchdog_loop():
    """每天 00:05 检查今天的 00:00 巡检是否**已启动**；未启动打 WARNING。

    00:00 巡检可能因线程异常或调度循环问题从未触发而缺席，光靠 00:00 时点
    的触发日志无法发现（08-09/08-10 无 full scan 完成日志即是）。已启动但
    被 ``_sync_lock`` 阻塞未完成不算异常（不误告警）。此 loop 跨日重置，
    每日一次，发现未启动即告警（供钉钉消费）。
    """
    last_date = None
    while not _stop.is_set():
        now = _dt.datetime.now()
        if (now.time() >= _dt.time(0, 5) and now.time() < _dt.time(0, 6)
                and now.date() != last_date):
            last_date = now.date()
            _warn_if_full_scan_incomplete(_scheduler_state, now)
        time.sleep(30)


def _midnight_clear_loop(data_sources) -> None:
    """次日 00:00 清空当日分钟内存库（前一日网络实时数据不跨日驻留）。"""
    last_date = _dt.date.today()
    while not _stop.is_set():
        time.sleep(30)
        today = _dt.date.today()
        if today != last_date:
            if today > last_date:  # 日期前跳（改系统时钟）时也兜底
                data_sources.minute_store.clear()
                logger.info("stockdata minute store cleared at midnight (new day %s)", today)
            last_date = today


def _dayfile_sweep_loop(data_sources, interval: float = 10.0) -> None:
    """每 10s 清扫缓存：dayfile LRU 踢旧 + 结果缓存过期清理 + 归还空闲堆。

    纯后台清扫、访问时不清理（spec 2026-08-21-stockdata-daily-dayfile-lru-design
    第 1 节卸载规则）；异常只记日志不退出，下次循环继续。
    """
    trim = _malloc_trim()
    while not _stop.is_set():
        time.sleep(interval)
        try:
            evicted = data_sources.dayfile_cache.sweep()
            if evicted:
                logger.debug("stockdata dayfile cache sweep evicted %d files", evicted)
        except Exception:  # noqa: BLE001
            logger.warning("stockdata dayfile cache sweep failed", exc_info=True)
        try:
            # TTL 结果缓存（get_minute 大窗口帧等）过期清理不能只挂在 set 上：
            # 回测/批量任务结束后流量归零，最后几个大帧会永久驻留（实测 RSS
            # 停在 1.9GB 不回落），由本循环周期清掉。
            purged = data_sources.dedup.purge_expired()
            if purged:
                logger.debug("stockdata result cache purge expired %d entries", purged)
        except Exception:  # noqa: BLE001
            logger.warning("stockdata result cache purge failed", exc_info=True)
        if trim is not None:
            try:
                trim()
            except Exception:  # noqa: BLE001
                logger.warning("stockdata malloc_trim failed", exc_info=True)


def _malloc_trim():
    """glibc malloc_trim（Linux）：把已释放但未归还 OS 的堆页交还，RSS 回落。

    polars/pandas 大帧释放后 glibc 常把内存留在 arena 不还 OS，服务 RSS 长期
    停在峰值（匿名页 ~1.6GB）。无 glibc / 调用失败时返回 None 并跳过。
    """
    try:
        import ctypes

        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim.argtypes = [ctypes.c_size_t]
        libc.malloc_trim.restype = ctypes.c_int
        return lambda: libc.malloc_trim(0)
    except Exception:  # noqa: BLE001
        return None


def trigger_sync(kind: str, **params) -> dict:
    """手动触发同步（供 handler 调用）。

    kind: backfill|daily|etf_minute|stock_minute|adj_factor|financials|check_day|check_full
    check_day 需传 ``day``（YYYY-MM-DD）。
    """
    if kind == "backfill":
        threading.Thread(target=_backfill_loop, daemon=True).start()
    elif kind == "financials":
        def _fin() -> None:
            from app.services import tdx_financials
            res = tdx_financials.sync_financials(force=bool(params.get("force")))
            with _lock:
                _scheduler_state["financials_sync"] = res
        threading.Thread(target=_fin, daemon=True).start()
    elif kind == "check_day":
        threading.Thread(target=_run_check_day, args=(params["day"],),
                         daemon=True).start()
    elif kind == "check_full":
        threading.Thread(target=_run_check_full, daemon=True).start()
    else:
        threading.Thread(target=_run_sync, daemon=True).start()
    return {"ok": True}


def start_scheduler(data_sources=None) -> None:
    if _threads:
        return
    _stop.clear()
    targets = [_backfill_loop, _sync_cron_loop, _midnight_scan_loop,
               _full_scan_watchdog_loop, _idle_trim_loop]
    if data_sources is not None:
        targets.append(lambda: _midnight_clear_loop(data_sources))
        targets.append(lambda: _dayfile_sweep_loop(data_sources))
    for target in targets:
        t = threading.Thread(target=target, name=f"stockdata-{target.__name__}", daemon=True)
        t.start()
        _threads.append(t)
    logger.info("stockdata scheduler started")


def stop_scheduler() -> None:
    _stop.set()
