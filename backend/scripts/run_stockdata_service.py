# backend/scripts/run_stockdata_service.py
"""stock data 服务独立进程：TCP server + 自治调度（FastAPI 主进程托管守护）。"""
from __future__ import annotations

import logging
import os
import signal
import threading

from app.services.stockdata import scheduler
from app.services.stockdata.server import StockDataServer
from app.services.stockdata.sources import DataSources

HOST = os.getenv("STOCKDATA_HOST", "127.0.0.1")


def _port() -> int:
    try:
        return int(os.getenv("STOCKDATA_PORT", "") or 3322)
    except (TypeError, ValueError):
        return 3322


def main() -> None:
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
