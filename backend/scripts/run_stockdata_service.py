# backend/scripts/run_stockdata_service.py
"""stock data 服务独立进程：TCP server + 自治调度（FastAPI 主进程托管守护）。"""
from __future__ import annotations

import contextlib
import logging
import os
import signal
import threading

# polars 静态捆绑 rusty-jemalloc：释放的大帧以 MADV_FREE 惰性持有（smaps_rollup
# 的 LazyFree 计数，实测回测后 ~1.5GB），内核按需零成本回收、MemAvailable 不受
# 影响，属 RSS 表观滞留而非泄漏；该回收模式是编译期行为，env 无法改为强制归还。
# decay 置 0 只对 dirty 层（MADV_DONTNEED 即时归还）有效，保留无害。须在任何
# import 触发 jemalloc 初始化（即加载 polars）之前设置。
os.environ.setdefault("_RJEM_MALLOC_CONF", "dirty_decay_ms:0,muzzy_decay_ms:0")

from app.services.stockdata import scheduler
from app.services.stockdata.server import StockDataServer
from app.services.stockdata.sources import DataSources

HOST = os.getenv("STOCKDATA_HOST", "127.0.0.1")


def _limit_malloc_arenas() -> None:
    """glibc arena 上限压到 2：多线程回源默认 8×ncores 个 arena 会加剧堆碎片、
    RSS 停在高水位。mallopt 须在拉起线程池前调用；非 glibc 平台静默跳过。"""
    with contextlib.suppress(Exception):
        import ctypes

        ctypes.CDLL("libc.so.6").mallopt(-8, 2)  # M_ARENA_MAX


def _port() -> int:
    try:
        return int(os.getenv("STOCKDATA_PORT", "") or 3322)
    except (TypeError, ValueError):
        return 3322


def main() -> None:
    _limit_malloc_arenas()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    stop = threading.Event()

    def _sig(_signum, _frame) -> None:
        stop.set()
        scheduler.stop_scheduler()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    src = DataSources()
    server = StockDataServer((HOST, _port()), src)
    scheduler.start_scheduler(data_sources=src)  # backfill + 15:35 + 00:00 内存清空
    logger = logging.getLogger("stockdata")
    logger.info("stockdata service listening on %s:%s", HOST, _port())

    def _shutdown_loop() -> None:
        # shutdown() 必须在 serve_forever 之外的线程调用，否则死锁
        stop.wait()
        server.shutdown()

    threading.Thread(target=_shutdown_loop, daemon=True).start()
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
