import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

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


def run_sync(tmp_path, env=None, dirs=WHITELIST_DIRS, files=("trade_calendar.json",)):
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
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
        "STUB_STATS_LINE": _stats_samples()["有传输"],
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


def test_about_probe_uses_remote_with_colon(tmp_path):
    """预检必须探测 `remote:`（带冒号）；裸名字会被真实 rclone 当本地路径，报 not a remote。"""
    r = run_sync(tmp_path)
    abouts = [l.split("\t") for l in r["calls"].splitlines() if l.split("\t")[0] == "about"]
    assert abouts, "没有发起 about 预检"
    assert abouts[0][1].endswith(":")
    assert r["status"]["exit_code"] == 0


def test_about_failure_records_underlying_error(tmp_path):
    r = run_sync(tmp_path, env={"STUB_ABOUT_FAIL": "1"})
    assert r["status"]["exit_code"] == 4
    assert "stub-about-failed" in r["status"]["error_tail"]
    assert "stub-about-failed" in r["log"]


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
        # 节省：Drive 目录 mtime 同步是逐目录串行 API 调用（数百个分区 = 数分钟零吞吐），备份不需要
        assert "--no-update-dir-modtime" in l
        assert "--bwlimit" not in l                            # 未设 RCLONE_BWLIMIT 时不加
        assert not any(a.startswith("--delete") for a in l)
        assert l[0] not in ("sync", "move", "delete", "purge")  # 只看子命令，别匹配到 .../gdrive_sync.log
    srcs = {l[1] for l in copies if l[0] == "copy"}
    assert srcs == {str(tmp_path / "data" / d) for d in WHITELIST_DIRS}
    assert not any("quant.db" in a for l in copies for a in l)


def test_parallelism_env_overrides_reach_rclone_args(tmp_path):
    """并发度必须可调：Drive 侧限流时 4 并发是延迟瓶颈，靠提高并发把上行带宽用起来。"""
    r = run_sync(tmp_path, env={"RCLONE_TRANSFERS": "16", "RCLONE_CHECKERS": "24"})
    lines = [l.split("\t") for l in r["calls"].splitlines()]
    copies = [l for l in lines if l[0] in ("copy", "copyto")]
    assert copies
    for l in copies:
        assert l[l.index("--transfers") + 1] == "16"
        assert l[l.index("--checkers") + 1] == "24"
    # 默认值不变（用独立 tmp 目录：calls.log 在同一个 tmp_path 下是追加写的）
    r2 = run_sync(tmp_path / "d2")
    l2 = [l.split("\t") for l in r2["calls"].splitlines() if l.split("\t")[0] == "copy"][0]
    assert l2[l2.index("--transfers") + 1] == "4"
    assert l2[l2.index("--checkers") + 1] == "8"


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


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _stats_samples():
    """把 fixtures 里按 `# 名字` 分块的原文块读成 {名字: 块文本}。"""
    out, key = {}, None
    for line in (FIXTURES / "rclone_stats_block.txt").read_text().splitlines():
        if line.startswith("# "):
            key = line[2:].strip()
            out[key] = []
        elif key and line.strip():
            out[key].append(line)
    return {k: "\n".join(v) for k, v in out.items()}


def test_stats_parsed_from_real_rclone_samples(tmp_path):
    samples = _stats_samples()
    r = run_sync(tmp_path, env={"STUB_STATS_LINE": samples["有传输"]})
    assert r["status"]["exit_code"] == 0
    assert r["status"]["transferred_files"] >= 1
    assert r["status"]["transferred_bytes"] > 0
    r2 = run_sync(tmp_path / "second", env={"STUB_STATS_LINE": samples["无传输"]})
    assert r2["status"]["transferred_files"] == 0
    assert r2["status"]["transferred_bytes"] == 0
    assert r2["status"]["exit_code"] == 0


def test_stats_accumulate_across_units(tmp_path):
    """每个单元的收尾统计块都要累加——不能只算最后一个单元。"""
    samples = _stats_samples()
    (tmp_path / "one").mkdir()
    (tmp_path / "two").mkdir()
    one = run_sync(tmp_path / "one", dirs=["kline_minute"], files=(),
                   env={"STUB_STATS_LINE": samples["有传输"]})
    two = run_sync(tmp_path / "two", dirs=["kline_minute", "etf_nav"], files=(),
                   env={"STUB_STATS_LINE": samples["有传输"]})
    assert one["status"]["transferred_files"] == 4
    assert two["status"]["transferred_files"] == 8
    assert one["status"]["transferred_bytes"] > 0
    assert two["status"]["transferred_bytes"] == 2 * one["status"]["transferred_bytes"]


def test_unparsable_or_missing_stats_is_null_not_crash(tmp_path):
    r = run_sync(tmp_path, env={"STUB_STATS_LINE": "INFO  : nothing useful here"})
    assert r["proc"].returncode == 0
    assert r["status"]["transferred_files"] is None
    assert r["status"]["transferred_bytes"] is None


def test_corrupt_previous_status_does_not_crash(tmp_path):
    st = tmp_path / "prev.json"
    st.write_text("{not json")
    r = run_sync(tmp_path, env={"GDRIVE_STATUS": str(st)})
    assert r["proc"].returncode == 0
    assert r["status"]["last_success_at"] is not None


def test_error_tail_collects_this_run_errors_only(tmp_path):
    st = tmp_path / "prev.json"
    st.write_text(json.dumps({"last_success_at": "2026-10-17T23:31:00+08:00"}))
    r = run_sync(tmp_path, env={"STUB_FAIL_IN": "financials", "GDRIVE_STATUS": str(st)})
    assert r["status"]["exit_code"] == 5
    assert "financials" in r["status"]["error_tail"]
    assert len(r["status"]["error_tail"]) <= 500


def test_log_rotation_keeps_old_in_dot1_and_writes_run_lines_to_new_log(tmp_path):
    log = tmp_path / "data" / "gdrive_sync.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("OLD-MARKER\n" * 500)          # 远超阈值
    r = run_sync(tmp_path, env={"GDRIVE_LOG": str(log), "GDRIVE_LOG_MAX_BYTES": "100"})
    assert (log.parent / "gdrive_sync.log.1").read_text().count("OLD-MARKER") == 500
    assert "OLD-MARKER" not in r["log"]
    assert "[sync]" in r["log"]


@pytest.mark.integration
def test_e2e_remote_matches_local_and_restore_md5():
    """真实远端校验（需已授权 gdrive + 已完成首轮全量）。默认 -m integration 才跑。"""
    rclone = Path.home() / ".local/bin/rclone"
    if not rclone.exists():
        pytest.skip("rclone 未安装")
    data = Path(__file__).resolve().parents[2] / "data"
    remote = "gdrive:tickflow-stockdata/data"
    probe = subprocess.run([str(rclone), "about", "gdrive:"], capture_output=True, text=True)
    if probe.returncode != 0:
        pytest.skip("gdrive remote 未授权")
    newest = sorted((data / "kline_minute").glob("date=*"))[-1]
    rel = f"kline_minute/{newest.name}/part.parquet"
    src = data / rel
    if not src.exists():
        pytest.skip(f"本地缺少 {rel}")
    local_md5 = hashlib.md5(src.read_bytes()).hexdigest()
    out = Path("/tmp/opencode/gdrive_restore_probe.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run([str(rclone), "copyto", f"{remote}/{rel}", str(out)],
                          capture_output=True, text=True, timeout=600)
    assert proc.returncode == 0, proc.stderr
    assert out.stat().st_size == src.stat().st_size
    assert hashlib.md5(out.read_bytes()).hexdigest() == local_md5
