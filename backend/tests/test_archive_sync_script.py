import json
import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "sync_archive_to_gdrive.sh"

# 归档根目录（一次性历史数据，不进每日增量）
ARCHIVE_SOURCES = [
    "/home/caisl/A股_分时数据_指数",
    "/home/caisl/A股分钟数据",
]
STUB_SRC = """#!/bin/bash
sub="$1"; shift
printf '%s' "$sub" >> "$STUB_RCLONE_CALLS"
for a in "$@"; do printf '\\t%s' "$a" >> "$STUB_RCLONE_CALLS"; done
printf '\\n' >> "$STUB_RCLONE_CALLS"
case "$sub" in
  version) echo "rclone v1.71.0-stub"; exit 0 ;;
  about) if [ "${STUB_ABOUT_FAIL:-0}" = "1" ]; then echo "stub-about-failed" >&2; exit 1; fi
         case "${1:-}" in
           *:) ;;
           *) echo "about: not a remote: ${1:-}" >&2; exit 1 ;;
         esac
         echo "Total: 15 GiB"; exit 0 ;;
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


def run_sync(tmp_path, env=None, sources=ARCHIVE_SOURCES):
    stub = tmp_path / "rclone"
    stub.write_text(STUB_SRC)
    stub.chmod(0o755)
    calls = tmp_path / "calls.log"
    full_env = {
        "PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", ""),
        "RCLONE_BIN": str(stub),
        "ARCHIVE_SOURCES": "\n".join(sources),
        "GDRIVE_REMOTE": "gdrive:tickflow-stockdata/archive",
        "GDRIVE_LOG": str(tmp_path / "archive_sync.log"),
        "GDRIVE_STATUS": str(tmp_path / "archive_sync_status.json"),
        "GDRONE_LOCK": str(tmp_path / "archive.lock"),
        "GDRIVE_LOCK": str(tmp_path / "archive.lock"),
        "STUB_RCLONE_CALLS": str(calls),
        "STUB_STATS_LINE": "Transferred:   	          1 MiB / 1 MiB, 100%",
    }
    full_env.update(env or {})
    proc = subprocess.run(["bash", str(SCRIPT)], env=full_env, capture_output=True, text=True, timeout=120)
    status_path = Path(full_env["GDRIVE_STATUS"])
    log_path = Path(full_env["GDRIVE_LOG"])
    return {
        "proc": proc, "env": full_env,
        "status": json.loads(status_path.read_text()) if status_path.exists() else None,
        "log": log_path.read_text() if log_path.exists() else "",
        "calls": calls.read_text() if calls.exists() else "",
    }


def test_copies_every_archive_source_to_remote(tmp_path):
    r = run_sync(tmp_path)
    assert r["proc"].returncode == 0, r["proc"].stderr
    assert r["status"]["exit_code"] == 0
    assert r["status"]["dirs_ok"] == 2
    assert r["status"]["dirs_failed"] == []
    lines = [l.split("\t") for l in r["calls"].splitlines()]
    copies = [l for l in lines if l[0] == "copy"]
    assert len(copies) == 2
    assert {l[1] for l in copies} == set(ARCHIVE_SOURCES)
    # 远端路径 = GDRIVE_REMOTE + 源目录名
    for l in copies:
        assert l[2] == f"gdrive:tickflow-stockdata/archive/{Path(l[1]).name}"


def test_never_uses_sync_or_delete(tmp_path):
    r = run_sync(tmp_path)
    lines = [l.split("\t") for l in r["calls"].splitlines()]
    for l in lines:
        assert l[0] not in ("sync", "move", "delete", "purge")
        assert not any(a.startswith("--delete") for a in l)


def test_uses_measured_best_params(tmp_path):
    """实测最优：--transfers 16、--no-update-dir-modtime（省每目录一次串行 API 调用）、
    --min-age 15m（防传半写文件）。"""
    r = run_sync(tmp_path, env={"RCLONE_TRANSFERS": "16"})
    lines = [l.split("\t") for l in r["calls"].splitlines()]
    copies = [l for l in lines if l[0] == "copy"]
    for l in copies:
        assert l[l.index("--transfers") + 1] == "16"
        assert "--no-update-dir-modtime" in l
        # 静态归档不需要 min-age（默认 0）——留着会静默漏传 mtime 很新的文件
        assert l[l.index("--min-age") + 1] == "0"


def test_missing_source_is_failure_not_silent_success(tmp_path):
    """源缺失（磁盘未挂载/路径笔误）必须判失败。

    一次性备份没有 cron 兜底，若当成功处理会被监控成"备份成功"，
    last_success_at 照常推进 → 数据其实没备份。故必须 exit 5 且不推进。
    """
    r = run_sync(tmp_path, sources=["/nonexistent/dir"])
    assert r["proc"].returncode == 5
    assert r["status"]["exit_code"] == 5
    assert r["status"]["dirs_ok"] == 0
    assert r["status"]["last_success_at"] is None
    assert "本地源不存在" in r["log"]


def test_lock_busy_skips_without_touching_remote(tmp_path):
    """并发保护：已有实例在跑就整轮跳过，不发任何 copy。"""
    lock_path = tmp_path / "busy.lock"
    lock_path.touch()
    # 真的持住锁：另开一个进程 flock 住它
    holder = subprocess.Popen(
        ["flock", "-x", str(lock_path), "sleep", "30"],
    )
    try:
        import time
        time.sleep(0.5)   # 等 holder 拿到锁
        r = run_sync(tmp_path, env={"GDRIVE_LOCK": str(lock_path)})
    finally:
        holder.kill()
        holder.wait()
    assert r["proc"].returncode == 0
    assert r["status"]["exit_code"] == 0
    assert r["status"]["skipped"] == "lock_busy"
    assert "copy" not in r["calls"]


def test_about_probe_requires_colon(tmp_path):
    r = run_sync(tmp_path)
    abouts = [l.split("\t") for l in r["calls"].splitlines() if l.split("\t")[0] == "about"]
    assert abouts and abouts[0][1].endswith(":")


def test_about_failure_exits_4(tmp_path):
    r = run_sync(tmp_path, env={"STUB_ABOUT_FAIL": "1"})
    assert r["proc"].returncode == 4
    assert r["status"]["exit_code"] == 4
    assert "stub-about-failed" in r["status"]["error_tail"]


def test_failure_does_not_advance_last_success(tmp_path):
    st = tmp_path / "prev.json"
    st.write_text(json.dumps({"last_success_at": "2026-10-19T23:31:00+08:00"}))
    r = run_sync(tmp_path, env={"STUB_FAIL_IN": "A股分钟数据", "GDRIVE_STATUS": str(st)})
    assert r["status"]["exit_code"] == 5
    assert r["status"]["last_success_at"] == "2026-10-19T23:31:00+08:00"
    assert r["status"]["dirs_failed"] == ["A股分钟数据"]


def test_status_has_expected_fields(tmp_path):
    r = run_sync(tmp_path)
    st = r["status"]
    for k in ("last_run_at", "last_success_at", "exit_code", "dirs_ok", "dirs_failed",
              "transferred_files", "transferred_bytes", "elapsed_s", "remote", "error_tail"):
        assert k in st, k
    assert st["remote"] == "gdrive:tickflow-stockdata/archive"
    assert st["last_success_at"] is not None
