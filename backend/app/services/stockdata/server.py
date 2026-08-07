# backend/app/services/stockdata/server.py
"""ThreadingTCPServer：每连接一线程，连接内循环读帧→分发→响应。"""
from __future__ import annotations

import logging
import socketserver

from . import protocol
from .handlers import handle
from .sources import DataSources

logger = logging.getLogger("app.services.stockdata.server")


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:  # noqa: D102
        src: DataSources = self.server.data_sources  # type: ignore[attr-defined]
        try:
            while True:
                msg = protocol.decode_frame(self.request)
                req_id = msg["id"]
                try:
                    t, data = handle(msg["m"], msg.get("p") or {}, src)
                    resp = protocol.encode_response(req_id, True, t, data)
                except Exception as e:  # noqa: BLE001
                    logger.warning("[stockdata] method=%s 失败: %s", msg.get("m"), e)
                    resp = protocol.encode_response(
                        req_id, False, "json",
                        {"code": type(e).__name__, "msg": str(e)})
                self.request.sendall(resp)
        except (EOFError, ConnectionError):
            return
        except Exception:  # noqa: BLE001
            logger.exception("[stockdata] 连接处理异常")


class StockDataServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int], data_sources: DataSources):
        self.data_sources = data_sources
        super().__init__(addr, _Handler)
