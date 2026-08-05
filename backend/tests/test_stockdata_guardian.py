import os
import subprocess
import sys
import time
from pathlib import Path

from app.services.stockdata_guardian import StockDataGuardian


def test_guardian_restarts_died_process(tmp_path):
    script = tmp_path / "sleepy.py"
    script.write_text("import time, sys\ntime.sleep(60)\n")
    pidfile = tmp_path / "proc.pid"
    g = StockDataGuardian(pidfile=pidfile, script=script, logfile=tmp_path / "p.log")
    g.start()
    try:
        pid = int(pidfile.read_text().strip())
        assert os.path.exists(f"/proc/{pid}")
        os.kill(pid, 9)
        time.sleep(4.5)  # 3s poll + 余量
        new_pid = int(pidfile.read_text().strip())
        assert new_pid != pid
        assert os.path.exists(f"/proc/{new_pid}")
    finally:
        g.stop()
