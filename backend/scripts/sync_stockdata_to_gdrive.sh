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
  --log-level INFO
  --log-file "$GDRIVE_LOG"
)
if [ -n "$RCLONE_BWLIMIT" ]; then
  RCLONE_ARGS+=(--bwlimit "$RCLONE_BWLIMIT")
fi

# ---- 同步白名单（顺序即上传顺序；新增数据类型必须显式加进来）----
DIRS=(
  kline_minute kline_etf_minute kline_daily kline_etf_daily kline_index_daily
  kline_daily_enriched kline_etf_enriched kline_index_enriched
  adj_factor adj_factor_etf instruments instruments_etf instruments_index
  financials industries_sw etf_nav
)
FILES=(trade_calendar.json)

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

# 单个白名单单元：copy 目录 / copyto 单文件。返回值即 rclone 退出码。
# 绝不用 sync、绝不带 --delete*（远端只增不减）。
_copy_unit() {
  local rel="$1" mode="${2:-dir}"
  if [ "$mode" = "file" ]; then
    "$RCLONE_BIN" copyto "$GDRIVE_DATA_DIR/$rel" "$REMOTE_ROOT/$rel" "${RCLONE_ARGS[@]}"
  else
    "$RCLONE_BIN" copy "$GDRIVE_DATA_DIR/$rel" "$REMOTE_ROOT/$rel" "${RCLONE_ARGS[@]}"
  fi
}

# 本轮日志里的错误摘要（取本轮新增行，不看历史）
_compute_error_tail() {
  awk -v start="${LOG_START_LINES:-0}" 'NR>start' "$GDRIVE_LOG" 2>/dev/null \
    | grep -iE 'error|failed' | tail -n 5 | paste -sd ' | ' - | cut -c1-500
}

# 解析本轮最后一个 rclone 结尾统计块 → stdout "files bytes"；无法解析 → "null null"。
# 依据本机真实 rclone 输出（backend/tests/fixtures/rclone_stats_block.txt）：
# 只有完整统计块同时含「字节对」与「文件数对」；无传输时文件数对整行不出现。
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
        block = i          # 最后一个字节对行 = 最后一个统计块的起点

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

# 每个单元复制完立即累加该单元的收尾统计（串行执行，所以「最后一个块」就是本单元的）
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

# 原子写状态：先读旧状态沿用 last_success_at，再写 .tmp 后 mv
_write_status() {
  if ! command -v jq >/dev/null 2>&1; then
    printf '[%s] [sync] jq 不可用，无法写状态文件 %s\n' "$(_ts)" "$GDRIVE_STATUS" >&2
    return 0
  fi
  local now prev_success success tmp skipped_arg stats_args
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
  # 本轮统计解析失败（rclone 输出形态变了？）写 null，而不是写一个错值
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
  # 必须带冒号：裸名字会被 rclone 当成本地路径（not a remote）
  local host="${GDRIVE_REMOTE%%:*}" about_out about_tail
  if ! about_out="$("$RCLONE_BIN" about "$host:" 2>&1)"; then
    about_tail="$(printf '%s\n' "$about_out" | tail -n 3 | paste -sd ' | ' - | cut -c1-400)"
    STATUS_EXIT=4
    STATUS_ERROR_TAIL="about 探测失败: $host: | $about_tail"
    _log "授权/连通预检失败，退出 4: rclone about $host: → $about_tail"
    _write_status
    exit 4
  fi
  _log "授权预检通过（rclone about $host:）"

  # ---- 白名单增量复制：单个单元失败不中断整体 ----
  local rel rc
  for rel in "${DIRS[@]}"; do
    if [ ! -d "$GDRIVE_DATA_DIR/$rel" ]; then
      _log "跳过（本地不存在）: $rel"
      continue
    fi
    rc=0
    _copy_unit "$rel" dir || rc=$?
    _accumulate_stats
    if [ "$rc" = "0" ]; then
      DIRS_OK=$((DIRS_OK + 1))
      _log "单元完成: $rel"
    else
      DIRS_FAILED+=("$rel")
      _log "单元失败: $rel rc=$rc"
    fi
  done
  for rel in "${FILES[@]}"; do
    if [ ! -f "$GDRIVE_DATA_DIR/$rel" ]; then
      _log "跳过（本地不存在）: $rel"
      continue
    fi
    rc=0
    _copy_unit "$rel" file || rc=$?
    _accumulate_stats
    if [ "$rc" = "0" ]; then
      DIRS_OK=$((DIRS_OK + 1))
      _log "单元完成: $rel"
    else
      DIRS_FAILED+=("$rel")
      _log "单元失败: $rel rc=$rc"
    fi
  done

  if [ "${#DIRS_FAILED[@]}" -gt 0 ]; then
    STATUS_EXIT=5
  fi
  # 错误摘要取本轮日志（预检阶段已显式写入的摘要优先保留）
  local tail_out
  tail_out="$(_compute_error_tail)"
  if [ -n "$tail_out" ]; then
    STATUS_ERROR_TAIL="$tail_out"
  fi

  _write_status
  _log "结束 exit=$STATUS_EXIT dirs_ok=$DIRS_OK dirs_failed=${#DIRS_FAILED[@]} elapsed=${SECONDS}s"
  exit "$STATUS_EXIT"
}

main "$@"
