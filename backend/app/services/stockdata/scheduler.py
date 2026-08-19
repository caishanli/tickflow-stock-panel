# backend/app/services/stockdata/scheduler.py
"""自治调度：启动 backfill + 15:35 收盘批量同步 + 00:00 清空当日分钟内存（线程）。

无主动盘中全市场轮询——实时分钟只在客户端请求时按需回源（见 sources.get_realtime_snapshot）。"""
from __future__ import annotations

import datetime as _dt
import logging
import threading
import time

logger = logging.getLogger("app.services.stockdata.scheduler")

_scheduler_state: dict = {"last_backfill": None, "last_sync": None, "sync_job": None}
_lock = threading.Lock()
_stop = threading.Event()
_threads: list[threading.Thread] = []
_sync_lock = threading.Lock()  # 15:35 cron 与手动 trigger 串行


def _backfill_loop():
    try:
        from app.services import mootdx_service
        res = mootdx_service.backfill_to_now()
        with _lock:
            _scheduler_state["last_backfill"] = str(_dt.datetime.now())
            _scheduler_state["backfill_result"] = res
        logger.info("stockdata startup backfill done: %s", res)
    except Exception:  # noqa: BLE001
        logger.exception("stockdata startup backfill failed")


def _run_sync(full_stock_minute: bool = False):
    """收盘后同步（15:35 cron 传 full_stock_minute=True）。

    ``full_stock_minute``：收盘后整批把当天全市场股票分钟拉全（~2h），否则
    增量慢跑（每轮 limit=20）。手动 trigger_sync 保持增量 + 不落日线
    （盘中触发写半程日线会污染分区）。
    """
    with _sync_lock:
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
            logger.info("scheduled mootdx sync done: minute=%d rows, adj=%s, stock_minute_rows=%d, nav_rows=%d, daily=%s, index_daily=%s",
                        minutes, adj, stock, nav, daily, index_daily)
        except Exception:  # noqa: BLE001
            logger.exception("scheduled mootdx sync failed")


def _run_check_day(day: str) -> None:
    """单日检验补齐（后台线程）：解析日期并执行。"""
    with _sync_lock:
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


def _run_check_full() -> None:
    """全量检验补齐（后台线程）：执行并记录汇总。"""
    with _sync_lock:
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
    with _sync_lock:
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


def trigger_sync(kind: str, **params) -> dict:
    """手动触发同步（供 handler 调用）。

    kind: backfill|daily|etf_minute|stock_minute|adj_factor|check_day|check_full
    check_day 需传 ``day``（YYYY-MM-DD）。
    """
    if kind == "backfill":
        threading.Thread(target=_backfill_loop, daemon=True).start()
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
               _full_scan_watchdog_loop]
    if data_sources is not None:
        targets.append(lambda: _midnight_clear_loop(data_sources))
    for target in targets:
        t = threading.Thread(target=target, name=f"stockdata-{target.__name__}", daemon=True)
        t.start()
        _threads.append(t)
    logger.info("stockdata scheduler started")


def stop_scheduler() -> None:
    _stop.set()
