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


def _run_sync():
    with _sync_lock:
        try:
            from app.services import mootdx_service
            minutes = mootdx_service.sync_etf_minute()
            adj = mootdx_service.sync_adj_factor()
            stock = mootdx_service.sync_stock_minute(
                limit=mootdx_service.STOCK_MINUTE_BATCH_LIMIT)
            from app.services import etf_nav_service
            nav = etf_nav_service.sync_etf_nav()
            with _lock:
                _scheduler_state["last_sync"] = str(_dt.datetime.now())
                _scheduler_state["sync_result"] = {
                    "minute_rows": minutes, "adj": adj, "stock_minute_rows": stock,
                    "nav_rows": nav}
            logger.info("scheduled mootdx sync done: minute=%d rows, adj=%s, stock_minute_rows=%d, nav_rows=%d",
                        minutes, adj, stock, nav)
        except Exception:  # noqa: BLE001
            logger.exception("scheduled mootdx sync failed")


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
                threading.Thread(target=_run_sync, daemon=True).start()
        time.sleep(30)


def _run_full_scan_once() -> None:
    """00:00 全量缺失巡检 + 补全（单次执行体，与 15:35 用 _sync_lock 串行）。"""
    with _sync_lock:
        try:
            from app.services import mootdx_service
            res = mootdx_service.scan_and_backfill_full()
            with _lock:
                _scheduler_state["last_full_scan"] = str(_dt.datetime.now())
                _scheduler_state["full_scan_result"] = res
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


def trigger_sync(kind: str) -> dict:
    """手动触发同步（供 handler 调用）。kind: backfill|daily|etf_minute|stock_minute|adj_factor"""
    if kind == "backfill":
        threading.Thread(target=_backfill_loop, daemon=True).start()
    else:
        threading.Thread(target=_run_sync, daemon=True).start()
    return {"ok": True}


def start_scheduler(data_sources=None) -> None:
    if _threads:
        return
    _stop.clear()
    targets = [_backfill_loop, _sync_cron_loop, _midnight_scan_loop]
    if data_sources is not None:
        targets.append(lambda: _midnight_clear_loop(data_sources))
    for target in targets:
        t = threading.Thread(target=target, name=f"stockdata-{target.__name__}", daemon=True)
        t.start()
        _threads.append(t)
    logger.info("stockdata scheduler started")


def stop_scheduler() -> None:
    _stop.set()
