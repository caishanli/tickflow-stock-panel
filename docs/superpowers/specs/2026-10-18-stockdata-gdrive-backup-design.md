# 本地行情数据 Google Drive 备份同步设计（stockdata-gdrive-backup）

- 日期：2026-10-18
- 状态：已批准（2026-10-18，五节逐节确认）
- 目标环境：本机（Ubuntu 26.04 桌面机，`caisl@/home/caisl/tickflow-stock-panel`）

## 1. 背景与目标

`data/` 承载全部行情落地数据（Parquet 分区 + 少量 JSON/Parquet 资产），全部不入 git（`.gitignore` 排除 `data/`）。机器故障即等于数据全失，`操作说明书.md` §22 只要求「定期备份 `data/`」，但没有任何自动化设施。本设计补上「全量 + 每日增量」的异地备份。

现状实测（2026-10-18）：

| 项 | 实测值 |
|---|---|
| 同步范围总量 | **4.7 GiB / 7520 个文件** |
| 每日增量 | **≈19 MB**（股票分钟 15M + ETF 分钟 3M + 日线 0.14M） |
| `kline_minute` 跨度 | 2018-02-09 → 2026-10-16（565 个日期分区，日分区 15MB） |
| `kline_daily` 跨度 | 2023-04-19 → 2026-10-16（457 个日期分区） |
| 文件数分布 | `kline_etf_daily` 独占 5260 个（每日一文件），其余目录基本每日一文件 |
| 机器约束 | 无 sudo 密码；`rclone` 未安装；github / downloads.rclone.org / Google 可达；有 X + firefox（可做浏览器 OAuth）；用户 crontab 为空 |
| 数据写入时刻 | stockdata 服务**工作日 15:35** 收盘批量回源（ETF 分钟 + 前复权因子 + 一批股票分钟，全市场股票分钟最坏 ~2.2h）；服务启动时另有 `backfill_to_now()` 补历史 |

- 目标：
  1. 一次性把同步范围内的行情数据完整上传到 Google Drive。
  2. 之后每天两次（17:30 / 23:30）只传新增/变更文件，长期无人值守运行。
  3. 运行状态可观测（本地 JSON 状态 + 日志），失败能一眼看出「上次真正成功是什么时候」。
  4. 能按需原路恢复到本机（文档记一行命令即可，不写恢复脚本）。
- 非目标：
  - 同步 `quant.db` / `quant_sim`（模拟盘交易状态，活动 SQLite，热拷贝不一致）、`quant_*` 派生库、日志、缓存、`data/` 下其它运行态文件。
  - 失败告警（不接钉钉/QQ 通道）、开机漏跑补偿、恢复脚本、监控页面。
  - 双向同步 / 云端状态回灌本地。

## 2. 方案选型

| 方案 | 取舍 | 结论 |
|---|---|---|
| **A. `rclone copy` + 镜像目录布局**（远端与本地同构） | 天然增量（rclone 按 size+mtime 跳过已存在且相同的文件）、永不删远端、恢复即一条反向 `copy`；代价是 Drive 上呈现为目录树而非单个归档包 | **采用** |
| B. 每次把新增分区打成 tar 传大包 | Drive 界面整洁、单文件下载快；但增量粒度差、恢复要解包，且首次 4.7G 单文件上传中断需整包重传（rclone 单文件不续传） | 否决 |
| C. `rclone sync`（真镜像，删除远端多余项） | 云端严格等于本地；但本地任何临时缺文件/清理动作都会删掉云端备份，作为备份方向是自伤 | 否决 |

## 3. 工具与授权

1. **安装**：下载 rclone 官方 linux-amd64 静态二进制（`https://downloads.rclone.org/rclone-current-linux-amd64.zip`）解包到 `~/.local/bin/rclone`，`chmod +x`。不需要 sudo、不装 apt 包。记录版本号到状态 JSON/日志。
   - 不依赖 PATH：脚本与 cron 一律用绝对路径调用。
2. **授权**：remote 名 `gdrive`，用户**在自己终端**执行一次交互式授权（Agent 会话无 TTY，无法代做）：
   ```
   rclone config create gdrive drive scope=drive config_is_local=true
   ```
   rclone 打印授权 URL（`http://127.0.0.1:53682/auth?...`），浏览器（firefox）登录 Google 账号并同意 → 粘贴返回 code。token 落在 `~/.config/rclone/rclone.conf`，`chmod 600`。
   - 兜底：若 `config create` 的交互提示不易回答，改用向导 `rclone config`（n 新建 → name `gdrive` → type `drive` → scope 选 `drive` → 其余回车默认 → auto config `y`），效果相同。
   - scope 用 `drive`（全盘读写），不用 `drive.file`——后者只能看到 rclone 自己创建的文件，后续手工在 Drive 里整理/迁移会受限。
3. **验收**：`rclone about gdrive:` 返回配额信息即授权成功；`rclone lsd gdrive:` 能列出根目录。

## 4. 远端布局

远端根：`gdrive:tickflow-stockdata/`，其下 `data/` 与本地 `data/` 相对路径**逐字节同构**，使得任意子路径都能直接反向 `copy` 回本地：

```
gdrive:tickflow-stockdata/data/
  kline_minute/date=2026-10-16/part.parquet
  kline_etf_minute/…      kline_daily/…        kline_etf_daily/…
  kline_index_daily/…     kline_daily_enriched/…  kline_etf_enriched/…
  kline_index_enriched/…  adj_factor_etf/…     instruments/…
  instruments_etf/…       instruments_index/…  financials/…
  industries_sw/…         etf_nav/…            trade_calendar.json
```

**同步范围白名单（16 个目录 + 1 个文件，共 4.7 GiB / 7520 文件）**：

| 相对路径 | 本地体积 | 文件数 | 说明 |
|---|---|---|---|
| `kline_minute` | 4.1G | 565 | 全市场股票 1m（回源主数据） |
| `kline_etf_minute` | 393M | 132 | ETF 1m |
| `kline_daily` | 62M | 457 | 股票日线（volume 手） |
| `kline_etf_daily` | 48M | 5260 | ETF 日线（volume 股） |
| `kline_index_daily` | 6.1M | 292 | 指数日线 |
| `kline_daily_enriched` | 110M | 456 | 派生 enriched（后端可重算，但一并备份实现「取到即用」） |
| `kline_etf_enriched` | 404K | 10 | 同上 |
| `kline_index_enriched` | 9.0M | 291 | 同上 |
| `adj_factor_etf` | 639K | 2 | ETF 逐日前复权因子 |
| `adj_factor` | 0 | 0 | 本机当前为空（TickFlow 实时资产位），保留占位 |
| `instruments` | 188K | 1 | 股票标的表 |
| `instruments_etf` | 0 | 0 | 本机当前为空，保留占位 |
| `instruments_index` | 6.1K | 1 | 指数标的表 |
| `financials` | 1.8M | 8 | 财务数据 |
| `industries_sw` | 115K | 1 | 申万行业映射 |
| `etf_nav` | 425K | 43 | ETF 净值 |
| `trade_calendar.json` | 121K | 1 | 权威交易日历 |

**明确排除**：`quant.db*`、`quant_sim/`、`quant_kline/`、`quant_strategies/`、`quant_cache/`、`*.log*`、`*.lock`、`.raw_partition_locks/`、`cache/`、`pools/`、`auction_cache/`、`depth5/`、`ai_cache/`、`backtest_results/`、`.backtest_matrix_cache/`、`mootdx_sync_failures.csv`、`backfill_state.json`、`job_store/`、`research/`、`user_data/`、`screener_results/`。

范围是脚本内的一份**白名单常量**，新增数据类型需显式加入（避免将来把交易状态/日志误传上去）。

## 5. 同步脚本

新增 `backend/scripts/sync_stockdata_to_gdrive.sh`（与现有 `backend/scripts/stockdata_ctl.sh` 同风格：`set -euo pipefail`、`$(dirname "$0")` 推导路径、中文提示、绝对路径）。

职责与实现要点：

1. **配置**：`SRC_DATA="$ROOT/data"`、`REMOTE="${GDRIVE_REMOTE:-gdrive:tickflow-stockdata/data}"`、`RCLONE="${RCLONE_BIN:-$HOME/.local/bin/rclone}"`。remote 名与远端根都可用环境变量覆盖，便于换盘/换账号。
2. **单实例锁**：`flock -n` 锁文件 `data/.gdrive_sync.lock`。已在对跑时立即退出（exit 0，状态 JSON 记 `skipped=lock_busy`），避免 17:30 那次未跑完时 23:30 或手动运行叠加。
3. **前置检查**：rclone 二进制存在且可执行；`rclone about gdrive:` 通过（快速校验授权未失效）。任一失败 → 写状态 JSON（`exit_code=非0`）、追加日志、退出非 0，**不上传**。
4. **逐目录增量复制**（白名单循环）：
   ```
   "$RCLONE" copy "$SRC_DATA/$rel" "$REMOTE/$rel" \
     --min-age 15m \
     --exclude '*.lock' --exclude '.raw_partition_locks/**' \
     --transfers 4 --checkers 8 \
     --retries 3 --low-level-retries 10 \
     --drive-chunk-size 32M \
     --log-file "$LOG" --log-level INFO --stats 60s --stats-one-line
   ```
   - **`copy` 而非 `sync`，且不带任何 `--delete*`**：远端只增不减，脚本永远不可能删除云端备份。
   - **`--min-age 15m`**：跳过 15 分钟内被修改的文件——收盘批量回源正在落盘的 parquet 不会被半路传走。
   - `trade_calendar.json` 用单文件 `rclone copyto`。目录不存在时跳过并在日志记一行（`adj_factor` / `instruments_etf` 当前为空目录，`--create-empty-src-dirs` 不开，Drive 端不建空目录）。
   - 单个目录失败不中断整体（记录失败清单，最后汇总退出码：全部成功 0，任一失败非 0）。理由：备份不该因为一个目录的问题放弃其余目录。
5. **日志**：`data/gdrive_sync.log`（沿用 `backend.log`/`stockdata.log` 命名约定）。rclone 以 `INFO` 追加写；脚本自身的关键行（开始/跳过/每目录耗时/结束汇总）也写同一文件。脚本内做**简单轮转**：启动时若日志 > 10 MiB，重命名为 `.1`（覆盖旧 `.1`）。cron 的 stdout/stderr 一并重定向到该日志（`>>`），避免 cron 邮件堆积。
6. **状态 JSON**：`data/gdrive_sync_status.json`，每次运行**原子覆盖**（写 `.tmp` 后 `mv`）：
   ```json
   {
     "last_run_at": "2026-10-18T23:30:02+08:00",
     "last_success_at": "2026-10-18T23:31:47+08:00",
     "exit_code": 0,
     "trigger": "cron|manual|first-full",
     "skipped": null,
     "dirs_ok": 17, "dirs_failed": [],
     "transferred_files": 4, "transferred_bytes": 19876543,
     "elapsed_s": 105,
     "rclone_version": "rclone v1.71.0",
     "remote": "gdrive:tickflow-stockdata/data",
     "error_tail": ""
   }
   ```
   - `transferred_files/bytes` 从 rclone `--stats-one-line` 的最后一行汇总（形如 `Transferred: 4 / 4, 100%, 18.9 MiB`）解析；解析失败则该字段为 `null`（不影响退出码）。`rclone_version` 写运行时的真实值，示例中的版本仅为格式示意。
   - **`last_success_at` 仅在整体退出码为 0 时推进**——它才是「数据最后一次真正上云成功」的依据；失败运行只更新 `last_run_at` 与 `error_tail`。
7. **首次全量**：同一脚本、同样参数直接跑（`--min-age 15m` 对历史分区无影响）。为避免阻塞会话，用 `setsid` 后台执行，`tail -f data/gdrive_sync.log` 看进度。默认**不限速**；若上行被占满影响上网，脚本支持 `RCLONE_BWLIMIT`（设置后追加 `--bwlimit`，如 `8M`）。

## 6. 定时

写入当前用户 crontab（无需 sudo）：

```
30 17 * * *  /home/caisl/tickflow-stock-panel/backend/scripts/sync_stockdata_to_gdrive.sh >> /home/caisl/tickflow-stock-panel/data/gdrive_sync.log 2>&1
30 23 * * *  /home/caisl/tickflow-stock-panel/backend/scripts/sync_stockdata_to_gdrive.sh >> /home/caisl/tickflow-stock-panel/data/gdrive_sync.log 2>&1
```

- **每天都跑，不判断交易日**：非交易日无新增，整轮只剩「列远端清单 + 比对本地上次结果」，几秒结束；比引入交易日判断更不容易漏（周末补跑、服务启动 backfill 补出的历史数据都能兜住）。
- crontab 里用绝对路径、不加载用户环境（cron 环境极简，脚本自身负责所有路径推导）。
- 17:30 那次可能撞上 15:35 收盘回源尚未跑完：传到的就是当时已落定的部分，剩下由 23:30 那次补齐；`--min-age 15m` 保证不会传半写文件。

## 7. 验证与文档

**一次性验证（实现完成时必须全过，作为交付证据）**：

1. 授权：`rclone about gdrive:` 输出配额。
2. 首次全量后比对：远端 `rclone size "$REMOTE" --json` 的 `bytes/count` 与本地白名单 `du -sb` / 文件数逐目录一致（允许 Drive 端不建空目录导致 `count` 差异，逐目录核对）。
3. **真实恢复验证**：从远端拉回至少一个当日分区到临时目录（如 `kline_minute/date=2026-10-16/part.parquet`），与本地文件比对 md5 一致——证明备份可读、可恢复，而不是「看起来传上去了」。
4. 增量验证（**在隔离沙箱内做，不碰真实数据目录与真实备份路径**）：
   a. 对已同步的真实范围跑一次 `rclone copy --dry-run`，应报告 0 个待传文件（证明 size+mtime 去重生效）。
   b. 建沙箱源 `/tmp/opencode/gdrive_sync_selftest/`（两个小文件）+ 沙箱远端 `gdrive:tickflow-stockdata/_selftest/`，用同一套参数跑两轮：第一轮 `transferred_files=2`，第二轮 `0`；随后 `rclone purge` 清掉沙箱远端目录。既验证「只传新文件」也不留下垃圾。
   c. 真实增量的最终证据来自次日实盘数据：新增 `date=` 分区被上传、`transferred_files>0`、`last_success_at` 推进。
5. 锁与错误路径：并发手动跑两次，第二次应 `skipped=lock_busy`；临时把 remote 名改错，验证退出码非 0 且 `last_success_at` 不推进。
6. `shellcheck`（若本机有）或至少 `bash -n` 语法检查通过。

**文档**：

- 新增 `docs/` 一节说明备份范围、恢复步骤、排障（对齐 `操作说明书.md` §22 的「备份内容」条目）。
- `操作说明书.md` §22.2 补一小节：行情数据已自动备份至 Google Drive，含一行恢复命令
  `rclone copy gdrive:tickflow-stockdata/data/<相对路径> ./data/<相对路径>`。
- `AGENTS.md` 的「运行」/数据层附近补 1~2 行：备份脚本位置、cron 时刻、状态文件路径。

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 上传半写 parquet（收盘批量落盘中） | `--min-age 15m`；且 rclone 按 size 比对，文件后续变大下轮会自动重传 |
| 授权 token 过期/被撤销（Google 对未验证应用的 refresh token 有失效策略） | 每轮运行前 `rclone about` 预检，失败即 `exit_code≠0` 并留日志；`last_success_at` 停滞即可发现（已知取舍：不做主动告警） |
| 上传把上行占满，影响实盘/上网 | 默认不限速；`RCLONE_BWLIMIT` 可设（如 `8M`），改后无需重启任何服务 |
| 误把交易状态/日志传上去 | 白名单常量 + 明确排除清单；新增类型须显式加入 |
| Drive 免费额度 15G（当前范围 4.7G，日后分钟数据增长） | 状态 JSON 记录远端体积；接近阈值时可在 Drive 侧看配额（`rclone about gdrive:`），必要时只保留近 N 年 `kline_minute` 分区（本次不做） |
| 机器关机导致漏跑 | 已知取舍：不做开机补偿；下次定时运行或多跑一次手动即可补齐（rclone 增量天然追平） |
| 17:30 与 23:30 两轮叠加 | `flock -n` 单实例锁 |

## 9. 交付物清单

1. `~/.local/bin/rclone`（二进制，非仓库内容）。
2. `~/.config/rclone/rclone.conf`（OAuth token，`chmod 600`，非仓库内容）。
3. `backend/scripts/sync_stockdata_to_gdrive.sh`（新增，仓库内容）。
4. 用户 crontab 两条（机器状态，非仓库内容）。
5. `data/gdrive_sync.log`、`data/gdrive_sync_status.json`（运行态，`data/` 不入库）。
6. 文档更新：`docs/<新文件>.md`、`操作说明书.md` §22、`AGENTS.md`。
