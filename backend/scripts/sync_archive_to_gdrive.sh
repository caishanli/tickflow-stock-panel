#!/bin/bash
# 一次性历史归档 → Google Drive 备份（与每日增量备份脚本分开）
#
# 用法: ./sync_archive_to_gdrive.sh
# 退出码: 0 成功/跳过 | 3 依赖缺失 | 4 预检失败 | 5 部分单元失败
#
# 与 sync_stockdata_to_gdrive.sh 的区别:
#   - 源是 /home/caisl 下的历史归档目录（静态、几十~上百 GB），不是 data/ 里的每日增量
#   - 一次性后台跑，不挂 cron
#   - 独立锁文件，不影响每天 17:30/23:30 的正常增量备份
#
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ---- 可覆盖配置 ----
# 归档源目录，每行一个（分号/逗号也接受）
ARCHIVE_SOURCES="${ARCHIVE_SOURCES:-/home/caisl/A股_分时数据_指数
/home/caisl/A股分钟数据}"
GDRIVE_REMOTE="${GDRIVE_REMOTE:-gdrive:tickflow-stockdata/archive}"
RCLONE_BIN="${RCLONE_BIN:-$HOME/.local/bin/rclone}"
RCLONE_TRANSFERS="${RCLONE_TRANSFERS:-16}"
RCLONE_CHECKERS="${RCLONE_CHECKERS:-16}"
RCLONE_BWLIMIT="${RCLONE_BWLIMIT:-}"
GDRIVE_MIN_AGE="${GDRIVE_MIN_AGE:-15m}"
GDRIVE_LOG="${GDRIVE_LOG:-$ROOT/data/gdrive_archive_sync.log}"
GDRIVE_STATUS="${GDRIVE_STATUS:-$ROOT/data/gdrive_archive_sync_status.json}"
GDRIVE_LOCK="${GDRIVE_LOCK:-$ROOT/data/.gdrive_archive_sync.lock}"
GDRIVE_TRIGGER="${GDRIVE_TRIGGER:-manual}"
GDRIVE_LOG_MAX_BYTES="${GDRIVE_LOG_MAX_BYTES:-10485760}"

RCLONE_ARGS=(
  --min-age "$GDRIVE_MIN_AGE"
  --exclude '*.lock'
  --transfers "$RCLONE_TRANSFERS"
  --checkers "$RCLONE_CHECKERS"
  --no-update-dir-modtime
  --retries 3
  --low-level-retries 10
  --drive-chunk-size 32M
  --stats 60s
  --log-level INFO
  --log-file "$GDRIVE_LOG"
)
if [ -n "$RCLONE_BWLIMIT" ]; then
  RCLONE_ARGS+=(--bwlimit "$RCLONE_BWLIMIT")
fi

# ---- 状态 ----
STATUS_EXIT=0
STATUS_SKIPPED=""
STATUS_ERROR_TAIL=""
DIRS_OK=0
DIRS_FAILED=()
TRANSFERRED_FILES=0
TRANSFERRED_BYTES=0
STATS_PARSED=1
RCLONE_VERSION="unknown"
LOG_START_LINES=0
SECONDS=0

_ts() { date +%Y-%m-%dT%H:%M:%S%:z; }

_log() {
  printf '[%s] [archive] %s\n' "$(_ts)" "$*" | tee -a "$GDRIVE_LOG"
}

_dirs_failed_json() {
  if [ "${#DIRS_FAILED[@]}" -eq 0 ]; then
    printf '[]'
    return 0
  fi
  printf '%s\n' "${DIRS_FAILED[@]}" | jq -R . | jq -sc .
}

# 解析本轮最后一个 rclone 结尾统计块 → stdout "files bytes"
_parse_stats() {
  python3 - "$GDRIVE_LOG" "${LOG_START_LINES:-0}" <<'PY'
import re
import sys

path, start = sys.argv[1], int(sys.argv[2])
try:
    with open(path, encoding="utf-8", errors="replace") as fh:
        lines = fh.read().splitlines()[start:]
except OSError:
    print("null null")
    raise SystemExit(0)

sizes = re.compile(r"^\s*Transferred:\s+([0-9.]+)\s*([KMGTP]i?B|B)\s*/", re.IGNORECASE)
counts = re.compile(r"^\s*Transferred:\s+([0-9]+)\s*/\s*([0-9]+)")
units = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4, "pib": 1024**5}

block = None
for i, line in enumerate(lines):
    if sizes.match(line):
        block = i

if block is None:
    print("null null")
    raise SystemExit(0)

m = sizes.match(lines[block])
total = int(round(float(m.group(1)) * units[m.group(2).lower()]))
files = 0
for line in lines[block:block + 4]:
    mc = counts.match(line)
    if mc:
        files = int(mc.group(1))
        break
print(files, total)
PY
}

_accumulate_stats() {
  local out files bytes
  out="$(_parse_stats)"
  files="${out%% *}"
  bytes="${out##* }"
  if [ "$files" = "null" ] || [ "$bytes" = "null" ]; then
    STATS_PARSED=0
    return 0
  fi
  TRANSFERRED_FILES=$((TRANSFERRED_FILES + files))
  TRANSFERRED_BYTES=$((TRANSFERRED_BYTES + bytes))
}

_write_status() {
  if ! command -v jq >/dev/null 2>&1; then
    printf '[%s] [archive] jq 不可用，无法写状态文件 %s\n' "$(_ts)" "$GDRIVE_STATUS" >&2
    return 0
  fi
  local now prev_success success tmp skipped_arg stats_args
  now="$(_ts)"
  prev_success=""
  if [ -f "$GDRIVE_STATUS" ]; then
    prev_success="$(jq -r '.last_success_at // empty' "$GDRIVE_STATUS" 2>/dev/null || true)"
  fi
  if [ "$STATUS_EXIT" = "0" ] && [ -z "$STATUS_SKIPPED" ]; then
    success="$now"
  else
    success="$prev_success"
  fi
  if [ -n "$STATUS_SKIPPED" ]; then
    skipped_arg=(--arg skipped "$STATUS_SKIPPED")
  else
    skipped_arg=(--argjson skipped null)
  fi
  if [ "$STATS_PARSED" = "1" ]; then
    stats_args=(--argjson transferred_files "$TRANSFERRED_FILES" --argjson transferred_bytes "$TRANSFERRED_BYTES")
  else
    stats_args=(--argjson transferred_files null --argjson transferred_bytes null)
  fi
  tmp="$GDRIVE_STATUS.tmp"
  if jq -n \
      --arg last_run_at "$now" \
      --arg last_success_at "$success" \
      --argjson exit_code "$STATUS_EXIT" \
      --arg trigger "$GDRIVE_TRIGGER" \
      "${skipped_arg[@]}" \
      --argjson dirs_ok "$DIRS_OK" \
      --argjson dirs_failed "$(_dirs_failed_json)" \
      "${stats_args[@]}" \
      --argjson elapsed_s "$SECONDS" \
      --arg rclone_version "$RCLONE_VERSION" \
      --arg remote "$GDRIVE_REMOTE" \
      --arg error_tail "$STATUS_ERROR_TAIL" \
      '{last_run_at: $last_run_at, last_success_at: (if $last_success_at == "" then null else $last_success_at end),
        exit_code: $exit_code, trigger: $trigger, skipped: $skipped,
        dirs_ok: $dirs_ok, dirs_failed: $dirs_failed,
        transferred_files: $transferred_files, transferred_bytes: $transferred_bytes,
        elapsed_s: $elapsed_s, rclone_version: $rclone_version, remote: $remote,
        error_tail: $error_tail}' > "$tmp" 2>/dev/null; then
    mv -f "$tmp" "$GDRIVE_STATUS"
  else
    rm -f "$tmp"
    _log "状态文件写入失败: $GDRIVE_STATUS"
  fi
}

main() {
  mkdir -p "$(dirname "$GDRIVE_LOG")" "$(dirname "$GDRIVE_STATUS")" 2>/dev/null || true

  # ---- 单实例锁 ----
  exec 9>"$GDRIVE_LOCK"
  if ! flock -n 9; then
    STATUS_SKIPPED="lock_busy"
    STATUS_EXIT=0
    _write_status
    _log "已有实例在跑，跳过本轮（lock_busy）"
    exit 0
  fi

  # ---- 日志轮转 ----
  if [ -f "$GDRIVE_LOG" ] && [ "$(stat -c%s "$GDRIVE_LOG" 2>/dev/null || echo 0)" -gt "$GDRIVE_LOG_MAX_BYTES" ]; then
    mv -f "$GDRIVE_LOG" "$GDRIVE_LOG.1"
  fi
  if [ -f "$GDRIVE_LOG" ]; then
    LOG_START_LINES="$(wc -l < "$GDRIVE_LOG")"
  fi

  _log "开始归档同步（trigger=$GDRIVE_TRIGGER, remote=$GDRIVE_REMOTE, min_age=$GDRIVE_MIN_AGE）"

  # ---- 依赖预检 ----
  local missing=()
  command -v jq >/dev/null 2>&1 || missing+=("jq")
  command -v python3 >/dev/null 2>&1 || missing+=("python3")
  [ -x "$RCLONE_BIN" ] || missing+=("rclone($RCLONE_BIN)")
  if [ "${#missing[@]}" -gt 0 ]; then
    STATUS_EXIT=3
    STATUS_ERROR_TAIL="依赖缺失: ${missing[*]}"
    _log "依赖缺失，退出 3: ${missing[*]}"
    _write_status
    exit 3
  fi

  RCLONE_VERSION="$("$RCLONE_BIN" version 2>/dev/null | head -1)"
  [ -n "$RCLONE_VERSION" ] || RCLONE_VERSION="unknown"
  _log "rclone: $RCLONE_VERSION"

  # ---- 授权预检（必须带冒号）----
  local host about_out about_tail
  host="${GDRIVE_REMOTE%%:*}"
  if ! about_out="$("$RCLONE_BIN" about "$host:" 2>&1)"; then
    about_tail="$(printf '%s\n' "$about_out" | tail -n 3 | paste -sd ' | ' - | cut -c1-400)"
    STATUS_EXIT=4
    STATUS_ERROR_TAIL="about 探测失败: $host: | $about_tail"
    _log "授权/连通预检失败，退出 4: rclone about $host: → $about_tail"
    _write_status
    exit 4
  fi
  _log "授权预检通过（rclone about $host:）"

  # ---- 逐个归档源 copy（绝不用 sync、绝不带 --delete）----
  local src name rc
  while IFS= read -r src; do
    [ -z "$src" ] && continue
    if [ ! -d "$src" ]; then
      _log "跳过（本地不存在）: $src"
      continue
    fi
    name="$(basename "$src")"
    _log "开始归档: $src → $GDRIVE_REMOTE/$name"
    rc=0
    "$RCLONE_BIN" copy "$src" "$GDRIVE_REMOTE/$name" "${RCLONE_ARGS[@]}" || rc=$?
    _accumulate_stats
    if [ "$rc" = "0" ]; then
      DIRS_OK=$((DIRS_OK + 1))
      _log "归档完成: $name"
    else
      DIRS_FAILED+=("$name")
      _log "归档失败: $name rc=$rc"
    fi
  done <<< "$(printf '%s\n' "$ARCHIVE_SOURCES" | tr ',;' '\n\n')"

  if [ "${#DIRS_FAILED[@]}" -gt 0 ]; then
    STATUS_EXIT=5
  fi

  local tail_out
  tail_out="$(awk -v start="${LOG_START_LINES:-0}" 'NR>start' "$GDRIVE_LOG" 2>/dev/null \
    | grep -iE 'error|failed' | tail -n 5 | paste -sd ' | ' - | cut -c1-500)"
  if [ -n "$tail_out" ]; then
    STATUS_ERROR_TAIL="$tail_out"
  fi

  _write_status
  _log "结束 exit=$STATUS_EXIT dirs_ok=$DIRS_OK dirs_failed=${#DIRS_FAILED[@]} elapsed=${SECONDS}s"
  exit "$STATUS_EXIT"
}

main "$@"
