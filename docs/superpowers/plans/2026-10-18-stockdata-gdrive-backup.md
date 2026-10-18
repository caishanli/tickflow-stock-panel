# 本地行情数据 Google Drive 备份同步 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 写一个独立的备份脚本 + 两条用户 crontab，把 `data/` 里的行情数据全量上传到 Google Drive，之后每天两次只传增量。

**Architecture:** 单个 bash 脚本 `backend/scripts/sync_stockdata_to_gdrive.sh` 作为唯一执行体：单实例 flock → 日志轮转 → 依赖/授权预检 → 白名单逐单元 `rclone copy`（永不带 `--delete`）→ 原子写状态 JSON。调度在**用户 crontab**（17:30 / 23:30 每天），完全独立于 stockdata 服务与主后端进程。脚本的所有输入路径都可用环境变量覆盖，因此可以用「假 rclone」在 pytest 里做黑盒测试。

**Tech Stack:** bash（`flock`、`jq`）、rclone（Google Drive 后端，OAuth scope=drive）、pytest（`subprocess` + `tmp_path`，沿用 `backend/tests/test_stockdata_guardian.py` 的做法）。

**Spec:** `docs/superpowers/specs/2026-10-18-stockdata-gdrive-backup-design.md`（实现时以它为准，本计划只补它没固定的实现细节）

## Global Constraints

- remote 名 `gdrive`，OAuth scope `drive`；远端根 `gdrive:tickflow-stockdata/data`，其下与本地 `data/` 相对路径逐字节同构。
- **永不使用 `rclone sync`，永不出现任何 `--delete*` 参数**（远端只增不减）。
- 同步白名单 = 16 个目录 + 1 个文件（共 4.7 GiB / 7520 文件，2026-10-18 基线）：
  `kline_minute`、`kline_etf_minute`、`kline_daily`、`kline_etf_daily`、`kline_index_daily`、`kline_daily_enriched`、`kline_etf_enriched`、`kline_index_enriched`、`adj_factor`、`adj_factor_etf`、`instruments`、`instruments_etf`、`instruments_index`、`financials`、`industries_sw`、`etf_nav` + `trade_calendar.json`。
- rclone 固定参数：`--min-age 15m --exclude '*.lock' --exclude '.raw_partition_locks/**' --transfers 4 --checkers 8 --retries 3 --low-level-retries 10 --drive-chunk-size 32M`。
- cron（每天跑，不判断交易日，绝对路径）：`30 17 * * *`、`30 23 * * *`，stdout/stderr 追加 `data/gdrive_sync.log`。
- 运行态文件：日志 `data/gdrive_sync.log`、状态 `data/gdrive_sync_status.json`（原子写）、锁 `data/.gdrive_sync.lock`。三者都在 `data/`，不入库、不上传。
- 不使用 sudo；cron 环境极简，脚本内所有路径自行推导，所有外部命令用绝对路径或前置依赖检查。
- 明确不进同步范围：`quant.db*`、`quant_sim`、`quant_kline`、`quant_strategies`、`quant_cache`、`*.log*`、`*.lock`、`.raw_partition_locks`、`cache`、`pools`、`auction_cache`、`depth5`、`ai_cache`、`backtest_results`、`.backtest_matrix_cache`、`job_store`、`research`、`user_data`、`screener_results`、`mootdx_sync_failures.csv`、`backfill_state.json`。
- 状态 JSON 语义：`last_success_at` **仅在本轮整体退出码为 0 时推进**；失败运行只更新 `last_run_at` 与 `error_tail`。

## Review Focus

以下五类输入/故障是 spec 隐含但没有任务默认覆盖的，每条都已挂到所属任务的测试里：

1. **无新数据时 rclone 的汇总行没有百分比数字**（形如 `Transferred: 0 / 0, -, 0 B`）→ 解析不能崩、不能写出错误值（Task 3）。
2. **某个白名单单元 rclone 非 0 退出**（单目录损坏/网络抖动）→ 不能中断其余单元，且 `last_success_at` 不得推进（Task 2）。
3. **`GDRIVE_REMOTE` 被覆盖成带尾斜杠或换盘名**（用户换账号/换盘）→ 拼接不得出现双斜杠或丢层级（Task 2）。
4. **上一轮状态 JSON 被人工删掉或损坏** → 本轮不得因此崩溃，`last_success_at` 写 `null` 而不是错值（Task 3）。
5. **轮转与 cron 的追加句柄共存** → 轮转后脚本自身的日志行必须落进新 `data/gdrive_sync.log`（Task 1）。

---

### Task 1: 脚本骨架（配置 / 锁 / 轮转 / 预检 / 状态）

**Files:**
- Create: `backend/scripts/sync_stockdata_to_gdrive.sh`
- Test: `backend/tests/test_gdrive_sync_script.py`

**Interfaces:**
- Consumes: 无（首个任务）
- Produces:
  - 可执行脚本 `backend/scripts/sync_stockdata_to_gdrive.sh`（无参数；退出码 0 成功、3 依赖缺失、4 预检失败、5 部分单元失败）
  - 环境变量入参（默认值）：`GDRIVE_DATA_DIR=$ROOT/data`、`GDRIVE_REMOTE=gdrive:tickflow-stockdata/data`、`RCLONE_BIN=$HOME/.local/bin/rclone`、`RCLONE_BWLIMIT=`（空=不限速）、`GDRIVE_MIN_AGE=15m`、`GDRIVE_LOG=$GDRIVE_DATA_DIR/gdrive_sync.log`、`GDRIVE_STATUS=$GDRIVE_DATA_DIR/gdrive_sync_status.json`、`GDRIVE_LOCK=$GDRIVE_DATA_DIR/.gdrive_sync.lock`、`GDRIVE_TRIGGER=cron`、`GDRIVE_LOG_MAX_BYTES=10485760`
  - 状态 JSON 字段名（后续任务沿用，不可改名）：`last_run_at`、`last_success_at`、`exit_code`、`trigger`、`skipped`、`dirs_ok`、`dirs_failed`、`transferred_files`、`transferred_bytes`、`elapsed_s`、`rclone_version`、`remote`、`error_tail`
  - 日志行格式：`[2026-10-18T23:30:02+08:00] [sync] <msg>`；时间戳 `date +%Y-%m-%dT%H:%M:%S%:z`
  - 供测试复用的假 rclone：`$STUB_RCLONE_CALLS` 记录每次调用的 `subcommand<TAB>arg<TAB>...` 一行；`copy`/`copyto` 把 `$STUB_STATS_LINE` 追加到 `--log-file` 指定文件；`$STUB_ABOUT_FAIL=1` 时 `about` 返回 1

- [ ] **Step 1: 写测试骨架与 4 个失败测试**

创建 `backend/tests/test_gdrive_sync_script.py`，内容如下（后续任务的测试追加到同一文件）：

```python
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
    holder = subprocess.Popen(["flock", str(lock), "sleep", "5"])
    try:
        import time
        time.sleep(0.5)
        r = run_sync(tmp_path, env={"GDRIVE_LOCK": str(lock)})
    finally:
        holder.kill()
    assert r["proc"].returncode == 0
    assert r["status"]["skipped"] == "lock_busy"
    assert r["calls"] == ""


def test_log_rotation_keeps_old_in_dot1_and_writes_run_lines_to_new_log(tmp_path):
    log = tmp_path / "data" / "gdrive_sync.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("OLD-MARKER\n" * 500)          # 远超阈值
    r = run_sync(tmp_path, env={"GDRIVE_LOG": str(log), "GDRIVE_LOG_MAX_BYTES": "100"})
    assert (log.parent / "gdrive_sync.log.1").read_text().count("OLD-MARKER") == 500
    assert "OLD-MARKER" not in r["log"]
    assert "[sync]" in r["log"]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/test_gdrive_sync_script.py -q`
Expected: 4 个用例 FAIL（脚本文件不存在 / `bash: ... No such file or directory`）

- [ ] **Step 3: 实现脚本骨架**

`backend/scripts/sync_stockdata_to_gdrive.sh` 结构固定为：

```bash
#!/bin/bash
# 行情数据 → Google Drive 备份同步（本地单向增量）。设计与验证见
# docs/superpowers/specs/2026-10-18-stockdata-gdrive-backup-design.md
# 用法: ./sync_stockdata_to_gdrive.sh   （无参数；cron 每天 17:30 / 23:30 调用）
# 退出码: 0 成功/跳过 | 3 依赖缺失 | 4 预检失败 | 5 部分单元失败
set -uo pipefail   # 故意不用 -e：按设计单个单元失败不中断整体，所有可失败调用显式判 rc
```

必须固定的实现点（其余自由）：

- `ROOT="$(cd "$(dirname "$0")/../.." && pwd)"`；上面 Interfaces 列的 10 个环境变量全部支持覆盖，默认值按 Interfaces 写。
- `_log()` 用 `>>` **每次重新打开**日志文件追加 `[ts] [sync] msg`（不持有长驻 fd——轮转后新的日志行必须落进新文件），同时 `echo` 到 stdout。
- 脚本内**不做** `--delete`/`sync`（Task 2 落实），本任务只需把参数数组 `RCLONE_ARGS` 建好：`--min-age "$GDRIVE_MIN_AGE"`、`--exclude '*.lock'`、`--exclude '.raw_partition_locks/**'`、`--transfers 4`、`--checkers 8`、`--retries 3`、`--low-level-retries 10`、`--drive-chunk-size 32M`、`--stats 60s`、`--log-level INFO`、`--log-file "$GDRIVE_LOG"`，`RCLONE_BWLIMIT` 非空时追加 `--bwlimit "$RCLONE_BWLIMIT"`。
- 主流程顺序：`exec 9>"$GDRIVE_LOCK"; flock -n 9` 失败 → 写状态（`skipped=lock_busy`、`exit_code=0`、沿用上次 `last_success_at`）→ exit 0。拿到锁后：日志轮转（`[ -f "$GDRIVE_LOG" ] && [ "$(stat -c%s "$GDRIVE_LOG")" -gt "$GDRIVE_LOG_MAX_BYTES" ] && mv -f "$GDRIVE_LOG" "$GDRIVE_LOG.1"`）→ 记录 `LOG_START_LINES=$(wc -l < "$GDRIVE_LOG" 2>/dev/null || echo 0)` → 预检 → （Task 2 的复制循环）→ 写状态 → 按汇总退出。
- 预检：`command -v jq`、`command -v python3` 与 `[ -x "$RCLONE_BIN" ]` 任一不满足 → `_log` 说明 + 写状态（`exit_code=3`）+ exit 3；`"$RCLONE_BIN" version | head -1` 捕获 `RCLONE_VERSION`（失败记 `unknown`）；`"$RCLONE_BIN" about "${GDRIVE_REMOTE%%:*}"` 非 0 → 写状态（`exit_code=4`，`error_tail` 记 `about 探测失败`）+ exit 4。
- `_write_status()`：先读旧状态取 `prev_success`（`jq -r '.last_success_at // empty'`，文件缺失或解析失败都当空，**不得因此崩**），再 `jq -n` 生成完整 JSON 写到 `$GDRIVE_STATUS.tmp` 后 `mv -f` 覆盖。`last_success_at` = 本轮 `exit_code==0` 时取当前时间戳，否则取 `prev_success`（为空写 `null`）；`skipped`、`error_tail` 无值写 `null`/空串；`dirs_failed` 写 JSON 数组；`transferred_files`/`transferred_bytes`/`elapsed_s` 本任务先写 `0`（Task 3 落实真实值）；`elapsed_s` 用 bash `SECONDS` 内置变量。

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/test_gdrive_sync_script.py -q`
Expected: 4 passed

- [ ] **Step 5: 语法检查并提交**

```bash
cd backend && bash -n scripts/sync_stockdata_to_gdrive.sh && chmod +x scripts/sync_stockdata_to_gdrive.sh
command -v shellcheck >/dev/null && shellcheck -S warning scripts/sync_stockdata_to_gdrive.sh || echo "shellcheck 未安装，跳过（bash -n 已过）"
cd /home/caisl/tickflow-stock-panel && git add backend/scripts/sync_stockdata_to_gdrive.sh backend/tests/test_gdrive_sync_script.py
git commit -m "feat(ops): 行情数据 Google Drive 备份脚本骨架（锁/预检/状态/日志轮转）"
```

---

### Task 2: 白名单增量复制循环

**Files:**
- Modify: `backend/scripts/sync_stockdata_to_gdrive.sh`
- Test: `backend/tests/test_gdrive_sync_script.py`

**Interfaces:**
- Consumes: Task 1 的环境变量、`_log()`、`RCLONE_ARGS`、`_write_status()`、退出码约定
- Produces:
  - 脚本内两个常量数组：`DIRS`（16 个目录，顺序同 Global Constraints）、`FILES`（`trade_calendar.json`）
  - `_copy_unit(rel, mode)`：`mode=dir` 走 `"$RCLONE_BIN" copy "$GDRIVE_DATA_DIR/$rel" "$GDRIVE_REMOTE/$rel" "${RCLONE_ARGS[@]}"`，`mode=file` 走 `copyto` 加同名目标；返回 rclone 退出码
  - 汇总：`DIRS_OK`（成功单元数，int）、`DIRS_FAILED`（失败单元相对路径，字符串数组）

- [ ] **Step 1: 写失败测试**

追加到 `backend/tests/test_gdrive_sync_script.py`：

```python
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
    srcs = {l[2] for l in copies if l[0] == "copy"}
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
    dests = {l[3] for l in [x.split("\t") for x in r["calls"].splitlines()]
             if l[0] in ("copy", "copyto")}
    assert dests == {f"gdrive:my-backup/{d}" for d in WHITELIST_DIRS} | {"gdrive:my-backup/trade_calendar.json"}
    assert not any("//" in d for d in dests)


def test_missing_whitelisted_dir_is_skipped_not_failed(tmp_path):
    r = run_sync(tmp_path, dirs=[d for d in WHITELIST_DIRS if d != "financials"])
    assert r["proc"].returncode == 0
    assert r["status"]["exit_code"] == 0
    assert r["status"]["dirs_ok"] == 16 and r["status"]["dirs_failed"] == []
    assert "financials" in r["log"] and "跳过" in r["log"]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/test_gdrive_sync_script.py -k "arg_discipline or per_unit or remote_override or missing_whitelisted" -q`
Expected: 4 个新用例 FAIL（`dirs_ok` 为 0、无 copy 调用记录）

- [ ] **Step 3: 实现复制循环**

要求：
- 远端根**先去尾斜杠**再拼接：`REMOTE_ROOT="${GDRIVE_REMOTE%/}"`，`_copy_unit` 一律用 `"$REMOTE_ROOT/$rel"`。
- 目录单元前置 `[ -d "$GDRIVE_DATA_DIR/$rel" ]`，文件单元前置 `[ -f "$GDRIVE_DATA_DIR/$rel" ]`；不存在 → `_log "跳过（本地不存在）: $rel"` 并计入「跳过」（既不算 ok 也不算 failed）。
- 存在则调用 `_copy_unit`；`if ! _copy_unit ...; then DIRS_FAILED+=("$rel"); else DIRS_OK=$((DIRS_OK+1)); fi`——**单个单元失败继续下一个**，每单元记一行 `_log "单元完成/失败: $rel rc=$rc"`。
- 循环结束后统一 `exit_code`：`DIRS_FAILED` 非空 → 5，否则 0。

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/test_gdrive_sync_script.py -q`
Expected: 8 passed

- [ ] **Step 5: 提交**

```bash
git add backend/scripts/sync_stockdata_to_gdrive.sh backend/tests/test_gdrive_sync_script.py
git commit -m "feat(ops): 备份脚本白名单增量复制（单单元失败不中断）"
```

---

### Task 3: 汇总统计解析与错误摘要

**Files:**
- Modify: `backend/scripts/sync_stockdata_to_gdrive.sh`
- Test: `backend/tests/test_gdrive_sync_script.py`
- Create: `backend/tests/fixtures/rclone_stats_block.txt`

**Interfaces:**
- Consumes: Task 1/2 的 `GDRIVE_LOG`、`LOG_START_LINES`、`_write_status()`
- Produces: 状态 JSON 的真实 `transferred_files`（int）、`transferred_bytes`（int，字节）、`error_tail`（本轮 ERROR/Failed 行最后 5 条以 ` | ` 连接，截断 500 字符，无则空串）、`elapsed_s`（int）

- [ ] **Step 1: 抓取真实 rclone 汇总行做固定样本**

用本地后端（无需 Google 授权）实跑，抓两种真实输出（若 `~/.local/bin/rclone` 已存在可直接用它，否则用下面的临时副本）：

```bash
mkdir -p /tmp/rclone-stats-probe/src /tmp/rclone-stats-probe/dst
head -c 200000 /dev/urandom > /tmp/rclone-stats-probe/src/a.bin
curl -fsSLo /tmp/rclone-current.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip
unzip -oq /tmp/rclone-current.zip -d /tmp/rclone-current
R=/tmp/rclone-current/rclone-*-linux-amd64/rclone
$R copy /tmp/rclone-stats-probe/src /tmp/rclone-stats-probe/dst --stats 1s --stats-one-line --log-level INFO 2>&1 | tail -5   # 有传输
$R copy /tmp/rclone-stats-probe/src /tmp/rclone-stats-probe/dst --stats 1s --stats-one-line --log-level INFO 2>&1 | tail -5   # 无传输
```

把抓到的两行（尤其 `Transferred:` 所在行）原样写入 `backend/tests/fixtures/rclone_stats_block.txt`，每行一条，并加注释行 `# 有传输` / `# 无传输`。这两条是解析器的**唯一契约来源**——不要凭记忆猜 rclone 格式。

- [ ] **Step 2: 写失败测试**

追加到 `backend/tests/test_gdrive_sync_script.py`（`FIXTURES` 指向 `tests/fixtures`）：

```python
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _stats_samples():
    out = {}
    for line in (FIXTURES / "rclone_stats_block.txt").read_text().splitlines():
        if line.startswith("# "):
            key = line[2:].strip()
        elif line.strip():
            out[key] = line
    return out


def test_stats_line_parsed_from_real_rclone_samples(tmp_path):
    samples = _stats_samples()
    r = run_sync(tmp_path, env={"STUB_STATS_LINE": samples["有传输"]})
    assert r["status"]["exit_code"] == 0
    assert r["status"]["transferred_files"] >= 1
    assert r["status"]["transferred_bytes"] > 0
    r2 = run_sync(tmp_path, env={"STUB_STATS_LINE": samples["无传输"]})
    assert r2["status"]["transferred_files"] == 0
    assert r2["status"]["transferred_bytes"] == 0
    assert r2["status"]["exit_code"] == 0


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
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/test_gdrive_sync_script.py -q`
Expected: 新增 4 个用例 FAIL（`transferred_files` 恒为 0、`error_tail` 空）

- [ ] **Step 4: 实现解析**

- 用 `awk -v start="$LOG_START_LINES" 'NR>start && /Transferred:/ {last=$0} END{print last}'` 取**本轮**最后一条 `Transferred:` 行（不取历史行；这是不用 `tail` 全文件的原因）。
- 解析固定用 **python3 辅助**（bash 里做「带单位的字节数换算 + 计数对」太脆），契约：把该行通过 argv 传入，stdout 打印一行两段空格分隔的 `files bytes`；无法解析时打印 `null null`。换算支持 `B`/`KiB`/`MiB`/`GiB`/`TiB`（1024 进制，四舍五入取整）。解析器先用 Task 3 Step 1 抓到的两条真实样本做输入，两条都必须解析成功（无传输那条应为 `0 0`）。
- 状态 JSON 里 `transferred_files`/`transferred_bytes` 用 `jq -n --argjson` 注入，值为 `null` 时写 JSON `null`。
- `error_tail`：`awk -v start="$LOG_START_LINES" 'NR>start' "$GDRIVE_LOG" | grep -iE 'error|failed' | tail -n 5 | paste -sd ' | ' -`，再截断 500 字符。
- `elapsed_s`：`SECONDS`。

- [ ] **Step 5: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/test_gdrive_sync_script.py -q`
Expected: 12 passed

- [ ] **Step 6: 提交**

```bash
git add backend/scripts/sync_stockdata_to_gdrive.sh backend/tests/test_gdrive_sync_script.py backend/tests/fixtures/rclone_stats_block.txt
git commit -m "feat(ops): 备份脚本汇总统计解析与错误摘要"
```

---

### Task 4: 安装 rclone 并完成 Google Drive 授权（需要用户操作）

**Files:**
- 不改仓库文件（产物 `~/.local/bin/rclone`、`~/.config/rclone/rclone.conf` 均在仓库外）

**Interfaces:**
- Consumes: 无
- Produces: 可用的 `~/.local/bin/rclone` 与已授权的 remote `gdrive`（Task 1 的默认 `RCLONE_BIN` 依赖它）

- [ ] **Step 1: 安装 rclone 二进制到 `~/.local/bin`**

```bash
curl -fsSLo /tmp/rclone-current.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip
unzip -oq /tmp/rclone-current.zip -d /tmp/rclone-current
install -m 755 /tmp/rclone-current/rclone-*-linux-amd64/rclone ~/.local/bin/rclone
~/.local/bin/rclone version | head -1
```

Expected: 打印 `rclone v1.7x.x`（无 sudo，未装 apt 包）

- [ ] **Step 2: 暂停，请用户在自己终端执行授权**

**Agent 无 TTY，此步必须由用户在自己终端跑**。给用户这条命令与预期：

```bash
~/.local/bin/rclone config create gdrive drive scope=drive config_is_local=true
```

预期：rclone 打印授权 URL 并在浏览器（firefox）打开 → 登录 Google 账号 → 同意 → 回到终端粘贴返回的 code → 提示 `Remote config` 成功。
兜底：若交互提示不易回答，改用 `~/.local/bin/rclone config` 向导（n 新建 → name `gdrive` → type `drive` → scope 选 `drive` → 其余回车默认 → auto config `y`）。

停下等待用户回报完成。

- [ ] **Step 3: 验证授权并收紧权限**

```bash
chmod 600 ~/.config/rclone/rclone.conf
~/.local/bin/rclone about gdrive:
~/.local/bin/rclone lsd gdrive:
```

Expected: `about` 打印 `Total:`/`Used:`/`Free:`；`lsd` 列出根目录（当前应为空或仅有用户既有目录）。任一失败则回到 Step 2。

---

### Task 5: 首次全量上传 + 端到端验证

**Files:**
- Test: `backend/tests/test_gdrive_sync_script.py`（追加一个 `integration` 用例）
- 运行态产物：`data/gdrive_sync.log`、`data/gdrive_sync_status.json`

**Interfaces:**
- Consumes: Task 1–4 的脚本、rclone、已授权 remote
- Produces: 远端 `gdrive:tickflow-stockdata/data` 与本地白名单一致的镜像；一个可重复运行的 `integration` 验证用例

- [ ] **Step 1: 后台跑首轮全量**

```bash
cd /home/caisl/tickflow-stock-panel
setsid env GDRIVE_TRIGGER=first-full backend/scripts/sync_stockdata_to_gdrive.sh > /tmp/gdrive-first-full.out 2>&1 < /dev/null &
```

然后**另起命令**看进度（不要在同一个调用里 sleep 轮询）：

```bash
tail -n 5 data/gdrive_sync.log; ls -la data/gdrive_sync_status.json
```

Expected: 日志出现 17 个单元的处理行；跑完 `data/gdrive_sync_status.json` 的 `exit_code=0`、`dirs_ok=17`、`transferred_files≈7520`。4.7 GiB 首次上传耗时取决于上行带宽，可中断后重跑（rclone 增量续传）。

- [ ] **Step 2: 逐目录比对远端与本地（字节数 + 文件数）**

```bash
python3 - <<'PY'
import json, subprocess
dirs = "kline_minute kline_etf_minute kline_daily kline_etf_daily kline_index_daily kline_daily_enriched kline_etf_enriched kline_index_enriched adj_factor adj_factor_etf instruments instruments_etf instruments_index financials industries_sw etf_nav".split()
data, remote = "/home/caisl/tickflow-stock-panel/data", "gdrive:tickflow-stockdata/data"
bad = []
for d in dirs:
    local = int(subprocess.run(["bash","-c",f"find {data}/{d} -type f -printf '%s\\n' | awk '{{s+=$1}}END{{print s+0}}'"],capture_output=True,text=True).stdout)
    js = json.loads(subprocess.run(["/home/caisl/.local/bin/rclone","size",f"{remote}/{d}","--json"],capture_output=True,text=True).stdout)
    if js["bytes"] != local: bad.append((d, local, js["bytes"]))
print("MISMATCH", bad) if bad else print("ALL DIRS MATCH", len(dirs))
PY
```

Expected: `ALL DIRS MATCH 16`（`kline_etf_daily` 5260 个小文件比对慢，耐心等）

- [ ] **Step 3: 写 integration 恢复验证用例**

追加到 `backend/tests/test_gdrive_sync_script.py`：

```python
import pytest


@pytest.mark.integration
def test_e2e_remote_matches_local_and_restore_md5():
    """真实远端校验（需已授权 gdrive + 已完成首轮全量）。默认 -m integration 才跑。"""
    import hashlib
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
    local_md5 = hashlib.md5((data / rel).read_bytes()).hexdigest()
    out = Path("/tmp/opencode/gdrive_restore_probe.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(rclone), "copyto", f"{remote}/{rel}", str(out)], check=True)
    assert hashlib.md5(out.read_bytes()).hexdigest() == local_md5
```

- [ ] **Step 4: 跑 integration 用例**

Run: `cd backend && uv run --extra dev pytest tests/test_gdrive_sync_script.py -m integration -q`
Expected: 1 passed（md5 与本地一致——证明云端数据可读可恢复）

- [ ] **Step 5: 沙箱验证「只传新文件」**

```bash
mkdir -p /tmp/opencode/gdrive_sync_selftest/kline_minute/date=2026-01-01
echo data > /tmp/opencode/gdrive_sync_selftest/kline_minute/date=2026-01-01/part.parquet
cd /home/caisl/tickflow-stock-panel
env GDRIVE_DATA_DIR=/tmp/opencode/gdrive_sync_selftest GDRIVE_REMOTE=gdrive:tickflow-stockdata/_selftest \
    GDRIVE_STATUS=/tmp/opencode/selftest_status.json GDRIVE_LOG=/tmp/opencode/selftest.log GDRIVE_LOCK=/tmp/opencode/selftest.lock \
    GDRIVE_TRIGGER=manual backend/scripts/sync_stockdata_to_gdrive.sh; jq '.transferred_files,.exit_code' /tmp/opencode/selftest_status.json
```

再跑一次同一条命令。Expected: 第一轮 `transferred_files=1`，第二轮 `0`（size+mtime 去重生效）。收尾 `~/.local/bin/rclone purge gdrive:tickflow-stockdata/_selftest` 并确认状态 JSON 用 `jq` 读得通。

- [ ] **Step 6: 真实增量回归 + 错误路径实测**

```bash
cd /home/caisl/tickflow-stock-panel && env GDRIVE_TRIGGER=manual backend/scripts/sync_stockdata_to_gdrive.sh; echo "rc=$?"
jq '.' data/gdrive_sync_status.json
```

Expected: `rc=0`、`transferred_files=0`、`transferred_bytes=0`、`last_success_at` 推进到本次、`dirs_failed=[]`。

再实测一次错误路径（spec §7.5）：记下当前 `last_success_at`，然后用一个不存在的远端盘名跑一轮，最后还原：

```bash
jq -r '.last_success_at' data/gdrive_sync_status.json > /tmp/ls_before.txt
env GDRIVE_TRIGGER=manual GDRIVE_REMOTE=gdrive-nonexistent:tickflow-stockdata/data \
    backend/scripts/sync_stockdata_to_gdrive.sh; echo "rc=$?"
jq -r '.last_success_at,.exit_code,.error_tail' data/gdrive_sync_status.json
diff <(cat /tmp/ls_before.txt) <(jq -r '.last_success_at' data/gdrive_sync_status.json) && echo "last_success_at 未推进 ✓"
```

Expected: `rc≠0`（4 或 5——`about` 探测或逐单元失败）、`exit_code≠0`、`last_success_at` 与 `diff` 前完全一致。随后用正常命令再跑一轮，把状态文件恢复成成功态。

- [ ] **Step 7: 提交测试**

```bash
git add backend/tests/test_gdrive_sync_script.py
git commit -m "test(ops): 备份链路 integration 恢复校验（md5 比对）"
```

---

### Task 6: 安装 crontab 定时

**Files:**
- 不改仓库文件（改用户 crontab）

**Interfaces:**
- Consumes: Task 1–5 的脚本
- Produces: 两条每天运行的 cron 作业

- [ ] **Step 1: 备份现有 crontab 并幂等追加两行**

```bash
crontab -l > /tmp/crontab.backup.$(date +%Y%m%d) 2>/dev/null || true
( crontab -l 2>/dev/null | grep -v 'sync_stockdata_to_gdrive.sh'; \
  echo '30 17 * * * /home/caisl/tickflow-stock-panel/backend/scripts/sync_stockdata_to_gdrive.sh >> /home/caisl/tickflow-stock-panel/data/gdrive_sync.log 2>&1'; \
  echo '30 23 * * * /home/caisl/tickflow-stock-panel/backend/scripts/sync_stockdata_to_gdrive.sh >> /home/caisl/tickflow-stock-panel/data/gdrive_sync.log 2>&1' ) | crontab -
crontab -l
```

Expected: `crontab -l` 恰好列出这两条（重复执行本步不会产生重复行）

- [ ] **Step 2: 用 cron 等价环境验证一次**

cron 的环境极简（`PATH=/usr/bin:/bin`、无交互式 shell 配置），所以用 `env -i` + 最小 PATH 模拟：

```bash
env -i PATH=/usr/bin:/bin HOME="$HOME" /bin/sh -c 'cd / && /home/caisl/tickflow-stock-panel/backend/scripts/sync_stockdata_to_gdrive.sh >> /home/caisl/tickflow-stock-panel/data/gdrive_sync.log 2>&1'; echo "rc=$?"
tail -n 20 /home/caisl/tickflow-stock-panel/data/gdrive_sync.log
```

Expected: `rc=0`；日志出现本轮 `[sync]` 行，`jq '.exit_code,.dirs_ok' data/gdrive_sync_status.json` 为 `0` 与 `17`。若这里失败说明脚本依赖了交互式环境（必须修，否则 cron 里同样会失败）。

- [ ] **Step 3: 记录次日验证点（交付说明里写清）**

次日检查：`jq '.last_run_at,.last_success_at,.transferred_files' /home/caisl/tickflow-stock-panel/data/gdrive_sync_status.json` 的 `last_run_at` 应为当天 17:30 或 23:30 之后。这是 cron 真正生效的最终证据（本次会话内无法等待）。

---

### Task 7: 文档

**Files:**
- Create: `docs/2026-10-18-stockdata-gdrive-backup.md`
- Modify: `操作说明书.md`（§22 备份内容小节 + 目录）
- Modify: `AGENTS.md`（运行/数据层附近补 1~2 行）

**Interfaces:**
- Consumes: Task 1–6 的产物（脚本路径、cron 时刻、状态字段、白名单）
- Produces: 可直接照做的恢复步骤与排障说明

- [ ] **Step 1: 写 `docs/2026-10-18-stockdata-gdrive-backup.md`**

必须覆盖（内容从 spec + 实测结果取，不新增设计）：
- 一句话定位 + 指向 spec 路径
- 同步范围表（16 目录 + 1 文件，含「为什么不含 quant.db」一句）
- 远端布局与 remote 名
- 手动运行命令（含 `GDRIVE_TRIGGER=manual`）、日志与状态文件位置、状态字段含义（尤其 `last_success_at` 语义）
- **恢复步骤**：`~/.local/bin/rclone copy gdrive:tickflow-stockdata/data/<相对路径> ./data/<相对路径>`（单分区 / 单目录 / 全量三种示例）
- 排障：退出码含义表（0/3/4/5）、`skipped=lock_busy`、`rclone about gdrive:` 判授权、日志轮转文件位置
- 已知取舍：不告警、不补偿漏跑、当天数据最晚 23:30 上云

- [ ] **Step 2: 更新 `操作说明书.md`**

在 §22.2「备份内容」之下补一小节（例如 `### 22.2.1 行情数据自动备份（Google Drive）`），内容：已自动备份到 Google Drive、备份范围、恢复一行命令、状态文件路径；并在 §22 目录（约 1008 行附近的章节结构处）按现有写法补该小节标题。

- [ ] **Step 3: 更新 `AGENTS.md`**

在「运行」或「架构要点」数据层附近补 2 行：备份脚本绝对路径、cron 时刻（17:30/23:30）、状态文件 `data/gdrive_sync_status.json`、设计文档路径。

- [ ] **Step 4: 提交**

```bash
git add docs/2026-10-18-stockdata-gdrive-backup.md 操作说明书.md AGENTS.md
git commit -m "docs(ops): 行情数据 Google Drive 备份与恢复说明"
```

---

## 交付后清单（写进给用户的交付说明）

1. 远端 `gdrive:tickflow-stockdata/data` 与本地白名单一致（附 Task 5 的比对输出与 md5 恢复证据）。
2. `backend/scripts/sync_stockdata_to_gdrive.sh` + `backend/tests/test_gdrive_sync_script.py`（12 个单测 + 1 个 integration 用例）。
3. crontab 两条（附 `crontab -l` 输出）。
4. 次日检查点：`data/gdrive_sync_status.json` 的 `last_run_at` 应晚于当天 17:30。
5. 未做（经确认）：失败告警、开机补偿、恢复脚本。
