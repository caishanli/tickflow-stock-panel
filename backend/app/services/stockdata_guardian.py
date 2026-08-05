"""FastAPI 主进程托管 stock data 服务子进程：单实例 PID 锁 + 3s 守护自愈。"""
from __future__ import annotations

import contextlib
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

logger = logging.getLogger("app.services.stockdata_guardian")


class StockDataGuardian:
    """托管 ``scripts/run_stockdata_service.py``：崩了 3s 内自动重启。"""

    def __init__(self, pidfile: Path, script: Path, logfile: Path | None = None,
                 poll_interval: float = 3.0):
        self.pidfile = Path(pidfile)
        self.script = Path(script)
        self.logfile = Path(logfile) if logfile else Path(pidfile.parent) / "stockdata.log"
        self._poll_interval = poll_interval
        self.proc: subprocess.Popen | None = None
        self._stop = threading.Event()

    def _kill_orphan(self) -> None:
        if not self.pidfile.exists():
            return
        try:
            old = int(self.pidfile.read_text().strip())
        except (ValueError, OSError):
            self.pidfile.unlink(missing_ok=True)
            return
        if old and os.path.exists(f"/proc/{old}"):
            try:
                os.killpg(old, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                pass
            for _ in range(50):
                if not os.path.exists(f"/proc/{old}"):
                    break
                time.sleep(0.1)
            if os.path.exists(f"/proc/{old}"):
                try:
                    os.killpg(old, signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
                for _ in range(50):
                    if not os.path.exists(f"/proc/{old}"):
                        break
                    time.sleep(0.1)
        self.pidfile.unlink(missing_ok=True)

    def _spawn(self) -> None:
        self.pidfile.parent.mkdir(parents=True, exist_ok=True)
        logf = open(self.logfile, "a")
        self.proc = subprocess.Popen(
            [sys.executable, str(self.script)],
            stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.pidfile.write_text(str(self.proc.pid))

    def _watch(self) -> None:
        while not self._stop.is_set():
            time.sleep(self._poll_interval)
            if self._stop.is_set():
                return
            if self.proc is None or self.proc.poll() is not None:
                if self._stop.is_set():
                    return
                logger.warning("stockdata service died, respawning")
                self._spawn()

    def start(self) -> None:
        self._kill_orphan()
        self._spawn()
        threading.Thread(target=self._watch, name="stockdata-guard",
                         daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        if self.proc is not None and self.proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
                os.killpg(self.proc.pid, signal.SIGTERM)
        self.pidfile.unlink(missing_ok=True)
