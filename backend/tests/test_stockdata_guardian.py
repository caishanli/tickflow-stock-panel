import os
import subprocess
import sys
import time
from pathlib import Path

from app.services.stockdata_guardian import StockDataGuardian, _trim_log_file


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


def test_trim_log_file_keeps_tail_half(tmp_path):
    log = tmp_path / "stockdata.log"
    log.write_bytes(b"".join(f"line-{i:05d}\n".encode() for i in range(2000)))
    size_before = log.stat().st_size
    _trim_log_file(log, max_bytes=10 * 1024)
    size_after = log.stat().st_size
    assert size_after < size_before
    assert size_after <= 5 * 1024 + 100  # 后半段(行对齐余量)
    tail = log.read_text()
    assert tail.endswith("line-01999\n")
    assert tail.splitlines()[0].startswith("line-")  # 首行为完整行(按行对齐)


def test_trim_log_file_below_limit_noop(tmp_path):
    log = tmp_path / "stockdata.log"
    log.write_text("a\nb\n")
    _trim_log_file(log, max_bytes=10 * 1024)
    assert log.read_text() == "a\nb\n"
