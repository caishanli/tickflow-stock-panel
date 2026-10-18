#!/bin/bash
# 行情数据 → Google Drive 备份同步（本地单向增量）。设计与验证见
# docs/superpowers/specs/2026-10-18-stockdata-gdrive-backup-design.md
# 用法: ./sync_stockdata_to_gdrive.sh   （无参数；cron 每天 17:30 / 23:30 调用）
# 退出码: 0 成功/跳过 | 3 依赖缺失 | 4 预检失败 | 5 部分单元失败
#
# 纪律：远端只增不减 —— 永不使用 rclone sync，永不出现任何 --delete* 参数。
set -uo pipefail   # 故意不用 -e：按设计单个单元失败不中断整体，所有可失败调用显式判 rc

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ---- 可覆盖配置（cron 环境极简，所有路径自行推导；测试靠这些变量做黑盒注入）----
GDRIVE_DATA_DIR="${GDRIVE_DATA_DIR:-$ROOT/data}"
GDRIVE_REMOTE="${GDRIVE_REMOTE:-gdrive:tickflow-stockdata/data}"
RCLONE_BIN="${RCLONE_BIN:-$HOME/.local/bin/rclone}"
RCLONE_BWLIMIT="${RCLONE_BWLIMIT:-}"
GDRIVE_MIN_AGE="${GDRIVE_MIN_AGE:-15m}"
GDRIVE_LOG="${GDRIVE_LOG:-$GDRIVE_DATA_DIR/gdrive_sync.log}"
GDRIVE_STATUS="${GDRIVE_STATUS:-$GDRIVE_DATA_DIR/gdrive_sync_status.json}"
GDRIVE_LOCK="${GDRIVE_LOCK:-$GDRIVE_DATA_DIR/.gdrive_sync.lock}"
GDRIVE_TRIGGER="${GDRIVE_TRIGGER:-cron}"
GDRIVE_LOG_MAX_BYTES="${GDRIVE_LOG_MAX_BYTES:-10485760}"

REMOTE_ROOT="${GDRIVE_REMOTE%/}"   # 去尾斜杠，避免拼接出双斜杠

RCLONE_ARGS=(
  --min-age "$GDRIVE_MIN_AGE"
  --exclude '*.lock'
  --exclude '.raw_partition_locks/**'
  --transfers 4
  --checkers 8
  --retries 3
  --low-level-retries 10
  --drive-chunk-size 32M
  --stats 60s
  --stats-one-line
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
RCLONE_VERSION="unknown"
LOG_START_LINES=0
SECONDS=0

_ts() { date +%Y-%m-%dT%H:%M:%S%:z; }

# 每次重新打开日志文件（不持长驻 fd）——轮转后新日志行必须落进新文件
_log() {
  printf '[%s] [sync] %s\n' "$(_ts)" "$*" | tee -a "$GDRIVE_LOG"
}

_dirs_failed_json() {
  if [ "${#DIRS_FAILED[@]}" -eq 0 ]; then
    printf '[]'
    return 0
  fi
  printf '%s\n' "${DIRS_FAILED[@]}" | jq -R . | jq -sc .
}

# 原子写状态：先读旧状态沿用 last_success_at，再写 .tmp 后 mv
_write_status() {
  if ! command -v jq >/dev/null 2>&1; then
    printf '[%s] [sync] jq 不可用，无法写状态文件 %s\n' "$(_ts)" "$GDRIVE_STATUS" >&2
    return 0
  fi
  local now prev_success success tmp skipped_arg
  now="$(_ts)"
  prev_success=""
  if [ -f "$GDRIVE_STATUS" ]; then
    prev_success="$(jq -r '.last_success_at // empty' "$GDRIVE_STATUS" 2>/dev/null || true)"
  fi
  # last_success_at 的语义 = 「数据最后一次真正上云成功」：只有真跑完且无跳过才推进
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
  tmp="$GDRIVE_STATUS.tmp"
  if jq -n \
      --arg last_run_at "$now" \
      --arg last_success_at "$success" \
      --argjson exit_code "$STATUS_EXIT" \
      --arg trigger "$GDRIVE_TRIGGER" \
      "${skipped_arg[@]}" \
      --argjson dirs_ok "$DIRS_OK" \
      --argjson dirs_failed "$(_dirs_failed_json)" \
      --argjson transferred_files "$TRANSFERRED_FILES" \
      --argjson transferred_bytes "$TRANSFERRED_BYTES" \
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
  mkdir -p "$GDRIVE_DATA_DIR" "$(dirname "$GDRIVE_LOG")" "$(dirname "$GDRIVE_STATUS")" 2>/dev/null || true

  # ---- 单实例锁：17:30 那轮没跑完时，23:30 / 手动运行直接跳过 ----
  exec 9>"$GDRIVE_LOCK"
  if ! flock -n 9; then
    STATUS_SKIPPED="lock_busy"
    STATUS_EXIT=0
    _write_status
    _log "已有实例在跑，跳过本轮（lock_busy）"
    exit 0
  fi

  # ---- 日志轮转（只在真正要跑时轮转）----
  if [ -f "$GDRIVE_LOG" ] && [ "$(stat -c%s "$GDRIVE_LOG" 2>/dev/null || echo 0)" -gt "$GDRIVE_LOG_MAX_BYTES" ]; then
    mv -f "$GDRIVE_LOG" "$GDRIVE_LOG.1"
  fi
  if [ -f "$GDRIVE_LOG" ]; then
    LOG_START_LINES="$(wc -l < "$GDRIVE_LOG")"
  fi

  _log "开始同步（trigger=$GDRIVE_TRIGGER, remote=$GDRIVE_REMOTE, min_age=$GDRIVE_MIN_AGE）"

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

  # ---- 授权/连通预检 ----
  local host="${GDRIVE_REMOTE%%:*}"
  if ! "$RCLONE_BIN" about "$host" >/dev/null 2>&1; then
    STATUS_EXIT=4
    STATUS_ERROR_TAIL="about 探测失败: $host"
    _log "授权/连通预检失败，退出 4: rclone about $host"
    _write_status
    exit 4
  fi
  _log "授权预检通过（rclone about $host）"

  # ---- 复制循环见 Task 2 ----

  _write_status
  _log "结束 exit=$STATUS_EXIT elapsed=${SECONDS}s"
  exit "$STATUS_EXIT"
}

main "$@"
