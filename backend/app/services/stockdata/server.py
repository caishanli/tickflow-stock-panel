# backend/app/services/stockdata/server.py
"""ThreadingTCPServer：每连接一线程，连接内循环读帧→分发→响应。"""
from __future__ import annotations

import logging
import os
import threading
import socketserver

from . import protocol
from .handlers import handle
from .sources import DataSources

logger = logging.getLogger("app.services.stockdata.server")


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        src: DataSources = self.server.data_sources  # type: ignore[attr-defined]
        try:
            while True:
                msg = protocol.decode_frame(self.request)
                req_id = msg["id"]
                try:
                    t, data = handle(msg["m"], msg.get("p") or {}, src)
                    resp = protocol.encode_response(req_id, True, t, data)
                except Exception as e:
                    logger.warning("[stockdata] method=%s 失败: %s", msg.get("m"), e)
                    resp = protocol.encode_response(
                        req_id, False, "json",
                        {"code": type(e).__name__, "msg": str(e)})
                self.request.sendall(resp)
        except (EOFError, ConnectionError):
            return
        except Exception:
            logger.exception("[stockdata] 连接处理异常")


class StockDataServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int], data_sources: DataSources):
        self.data_sources = data_sources
        self._readonly_server = None
        self._readonly_thread = None
        super().__init__(addr, _Handler)
        # Additive endpoint sharing the existing local reader. The TCP handler,
        # scheduler, launcher and Forward V1 activation manifest remain unchanged.
        if os.getenv("STOCKDATA_READONLY_UNIX", "1") == "1":
            try:
                from .readonly_unix import StockDataReadOnlyUnixServer
                self._readonly_server = StockDataReadOnlyUnixServer(data_sources)
                self._readonly_thread = threading.Thread(
                    target=self._readonly_server.serve_forever,
                    name="stockdata-readonly-unix", daemon=True)
                self._readonly_thread.start()
                logger.info("read-only Unix endpoint active")
            except Exception:
                if self._readonly_server is not None:
                    self._readonly_server.server_close()
                self._readonly_server = None
                self._readonly_thread = None
                logger.exception("read-only Unix endpoint unavailable; TCP preserved")

    def server_close(self):
        super().server_close()
        if self._readonly_server is not None:
            if self._readonly_thread is not None and self._readonly_thread.is_alive():
                self._readonly_server.shutdown()
                self._readonly_thread.join(timeout=2)
            self._readonly_server.server_close()
            self._readonly_server = None
