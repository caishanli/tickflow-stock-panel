import json
import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "sync_stockdata_to_gdrive.sh"
WHITELIST_DIRS = [
    "kline_minute", "kline_etf_minute", "kline_daily", "kline_etf_daily", "kline_index_daily",
    "kline_daily_enriched", "kline_etf_enriched", "kline_index_enriched",
    "adj_factor", "adj_factor_etf", "instruments", "instruments_etf", "instruments_index",
    "financials", "industries_sw", "etf_nav",
]
STUB_SRC = """#!/bin/bash
sub="$1"; shift
printf '%s' "$sub" >> "$STUB_RCLONE_CALLS"
for a in "$@"; do printf '\\t%s' "$a" >> "$STUB_RCLONE_CALLS"; done
printf '\\n' >> "$STUB_RCLONE_CALLS"
case "$sub" in
  version) echo "rclone v1.71.0-stub"; exit 0 ;;
  about) [ "${STUB_ABOUT_FAIL:-0}" = "1" ] && exit 1; echo "Total: 15 GiB"; exit 0 ;;
esac
logfile=""; prev=""
for a in "$@"; do [ "$prev" = "--log-file" ] && logfile="$a"; prev="$a"; done
[ -n "$logfile" ] && printf '%s\\n' "${STUB_STATS_LINE}" >> "$logfile"
if [ -n "${STUB_FAIL_IN:-}" ]; then
  for a in "$@"; do case "$a" in *"$STUB_FAIL_IN"*)
    [ -n "$logfile" ] && printf '%s\\n' "ERROR : $a: failed to copy" >> "$logfile"; exit 1;; esac; done
fi
exit 0
"""


def run_sync(tmp_path, env=None, dirs=WHITELIST_DIRS, files=("trade_calendar.json",)):
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    for d in dirs:
        (data / d).mkdir(exist_ok=True)
        (data / d / "part.parquet").write_bytes(b"x" * 10)
    for f in files:
        (data / f).write_text('{"dates": []}')
    stub = tmp_path / "rclone"
    stub.write_text(STUB_SRC)
    stub.chmod(0o755)
    calls = tmp_path / "calls.log"
    full_env = {
        "PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", ""),
        "RCLONE_BIN": str(stub), "GDRIVE_DATA_DIR": str(data),
        "GDRIVE_REMOTE": "gdrive:tickflow-stockdata/data",
        "GDRIVE_LOG": str(data / "gdrive_sync.log"),
        "GDRIVE_STATUS": str(data / "gdrive_sync_status.json"),
        "GDRIVE_LOCK": str(data / ".gdrive_sync.lock"),
        "STUB_RCLONE_CALLS": str(calls),
        "STUB_STATS_LINE": "2026/10/18 23:30:01 INFO  : Transferred:   2 / 2, 100%, 18.5 MiB, 1.2 MiB/s, ETA 0s",
    }
    full_env.update(env or {})
    proc = subprocess.run(["bash", str(SCRIPT)], env=full_env, capture_output=True, text=True, timeout=90)
    status_path = Path(full_env["GDRIVE_STATUS"])
    log_path = Path(full_env["GDRIVE_LOG"])
    return {
        "proc": proc, "env": full_env, "data": data,
        "status": json.loads(status_path.read_text()) if status_path.exists() else None,
        "log": log_path.read_text() if log_path.exists() else "",
        "calls": calls.read_text() if calls.exists() else "",
    }


def test_missing_rclone_exits_3_and_keeps_last_success(tmp_path):
    st = tmp_path / "prev.json"
    st.write_text(json.dumps({"last_success_at": "2026-10-17T23:31:00+08:00"}))
    r = run_sync(tmp_path, env={"RCLONE_BIN": str(tmp_path / "nope"), "GDRIVE_STATUS": str(st)})
    assert r["proc"].returncode == 3
    assert r["status"]["exit_code"] == 3
    assert r["status"]["last_success_at"] == "2026-10-17T23:31:00+08:00"
    assert r["calls"] == ""          # 依赖缺失时不得调用 rclone


def test_about_failure_exits_4_without_copy(tmp_path):
    r = run_sync(tmp_path, env={"STUB_ABOUT_FAIL": "1"})
    assert r["proc"].returncode == 4
    assert r["status"]["exit_code"] == 4
    assert r["status"]["last_success_at"] is None
    assert "copy" not in r["calls"]


def test_lock_busy_skips_and_does_not_touch_rclone(tmp_path):
    lock = tmp_path / "data" / ".gdrive_sync.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.touch()
    st = tmp_path / "prev.json"
    st.write_text(json.dumps({"last_success_at": "2026-10-17T23:31:00+08:00"}))
    holder = subprocess.Popen(["flock", str(lock), "sleep", "5"])
    try:
        import time
        time.sleep(0.5)
        r = run_sync(tmp_path, env={"GDRIVE_LOCK": str(lock), "GDRIVE_STATUS": str(st)})
    finally:
        holder.kill()
    assert r["proc"].returncode == 0
    assert r["status"]["skipped"] == "lock_busy"
    assert r["calls"] == ""
    # 跳过不是成功：什么都没传，last_success_at 必须停在上一轮
    assert r["status"]["last_success_at"] == "2026-10-17T23:31:00+08:00"


def test_full_success_copies_all_units_with_arg_discipline(tmp_path):
    r = run_sync(tmp_path)
    assert r["proc"].returncode == 0
    assert r["status"]["exit_code"] == 0
    assert r["status"]["dirs_ok"] == 17 and r["status"]["dirs_failed"] == []
    assert r["status"]["trigger"] == "cron"
    assert r["status"]["remote"] == "gdrive:tickflow-stockdata/data"
    assert r["status"]["last_success_at"] is not None
    lines = [l.split("\t") for l in r["calls"].splitlines()]
    copies = [l for l in lines if l[0] in ("copy", "copyto")]
    assert len(copies) == 17                                  # 16 目录 + 1 文件
    assert [l[0] for l in lines if l[0] == "copyto"] == ["copyto"]
    for l in copies:
        assert "--min-age" in l and "15m" in l
        assert "--transfers" in l and "--checkers" in l and "--retries" in l
        assert "--bwlimit" not in l                            # 未设 RCLONE_BWLIMIT 时不加
        assert not any(a.startswith("--delete") for a in l)
        assert l[0] not in ("sync", "move", "delete", "purge")  # 只看子命令，别匹配到 .../gdrive_sync.log
    srcs = {l[1] for l in copies if l[0] == "copy"}
    assert srcs == {str(tmp_path / "data" / d) for d in WHITELIST_DIRS}
    assert not any("quant.db" in a for l in copies for a in l)


def test_per_unit_failure_continues_and_does_not_advance_last_success(tmp_path):
    st = tmp_path / "prev.json"
    st.write_text(json.dumps({"last_success_at": "2026-10-17T23:31:00+08:00"}))
    r = run_sync(tmp_path, env={"STUB_FAIL_IN": "kline_index_daily", "GDRIVE_STATUS": str(st)})
    assert r["proc"].returncode == 5
    assert r["status"]["exit_code"] == 5
    assert r["status"]["dirs_failed"] == ["kline_index_daily"]
    assert r["status"]["dirs_ok"] == 16
    assert r["status"]["last_success_at"] == "2026-10-17T23:31:00+08:00"
    assert "etf_nav" in r["calls"]          # 失败单元之后的单元仍然跑
    assert "ERROR" in r["status"]["error_tail"]


def test_remote_override_with_trailing_slash_and_custom_host(tmp_path):
    r = run_sync(tmp_path, env={"GDRIVE_REMOTE": "gdrive:my-backup/"})
    dests = {l[2] for l in [x.split("\t") for x in r["calls"].splitlines()]
             if l[0] in ("copy", "copyto")}
    assert dests == {f"gdrive:my-backup/{d}" for d in WHITELIST_DIRS} | {"gdrive:my-backup/trade_calendar.json"}
    assert not any("//" in d for d in dests)


def test_missing_whitelisted_dir_is_skipped_not_failed(tmp_path):
    r = run_sync(tmp_path, dirs=[d for d in WHITELIST_DIRS if d != "financials"])
    assert r["proc"].returncode == 0
    assert r["status"]["exit_code"] == 0
    assert r["status"]["dirs_ok"] == 16 and r["status"]["dirs_failed"] == []
    assert "financials" in r["log"] and "跳过" in r["log"]


def test_log_rotation_keeps_old_in_dot1_and_writes_run_lines_to_new_log(tmp_path):
    log = tmp_path / "data" / "gdrive_sync.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("OLD-MARKER\n" * 500)          # 远超阈值
    r = run_sync(tmp_path, env={"GDRIVE_LOG": str(log), "GDRIVE_LOG_MAX_BYTES": "100"})
    assert (log.parent / "gdrive_sync.log.1").read_text().count("OLD-MARKER") == 500
    assert "OLD-MARKER" not in r["log"]
    assert "[sync]" in r["log"]
