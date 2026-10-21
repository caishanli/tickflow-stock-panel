# 历史归档数据 → Google Drive 一次性备份

- 日期：2026-10-20
- 脚本：`backend/scripts/sync_archive_to_gdrive.sh`
- 测试：`backend/tests/test_archive_sync_script.py`（9 个）
- 状态：`data/gdrive_archive_sync_status.json`　日志：`data/gdrive_archive_sync.log`
- 每日增量备份（另一套）：见 `docs/2026-10-18-stockdata-gdrive-backup.md`

## 备份什么

| 源 | 大小 | 文件数 | 远端路径 |
|---|---|---|---|
| `/home/caisl/A股_分时数据_指数` | 24 G | 31 086 | `gdrive:tickflow-stockdata/archive/A股_分时数据_指数/` |
| `/home/caisl/A股分钟数据` | 122 G | 157 | `gdrive:tickflow-stockdata/archive/A股分钟数据/` |
| **合计** | **146 G** | 31 243 | |

`A股_分时数据_指数` = 1/5/15/30/60 分钟 × `按月归档`（每交易日一 zip）+ `按年汇总`（每年一 zip）+ `指数列表_沪深京.csv`。
`A股分钟数据` = `1d_price.zip` + `1m_price_zip/` + `5m_price_zip/` + `4.21更新/`（含 60m/30m/15m、1d_feature、
calendar/stock_basic parquet）。

**注意**：这两套数据存在冗余（`4.21更新/` 是 `1m_price_zip/`+`5m_price_zip/` 的重新打包副本，
`按月归档` 是 `按年汇总` 的按日切片）——**按你的要求全部原样上传**，未做任何裁剪。远端会因此多占约 79 G。

## 手动运行

```bash
cd /home/caisl/tickflow-stock-panel
backend/scripts/sync_archive_to_gdrive.sh; echo "rc=$?"
```

可覆盖 env：`ARCHIVE_SOURCES`（每行一个源目录）、`GDRIVE_REMOTE`、`RCLONE_BIN`、
`RCLONE_TRANSFERS`/`RCLONE_CHECKERS`（默认 16）、`RCLONE_BWLIMIT`、`GDRIVE_MIN_AGE`（默认 **0**）、
`GDRIVE_LOG`/`GDRIVE_STATUS`/`GDRIVE_LOCK`、`GDRIVE_TRIGGER`、`GDRIVE_LOG_MAX_BYTES`。

> `GDRIVE_MIN_AGE` 默认 0：归档源是**静态**目录（最后改动 2026-10-03），没有
> "边写边读"的半写风险；留 15m 会静默漏传 mtime 很新的文件。若把
> `ARCHIVE_SOURCES` 指向**活动目录**，请手动设 `GDRIVE_MIN_AGE=15m`。

**不挂 cron**：源数据静态（最后改动 2026-10-03），一次性传完即可；以后要补传手动跑（增量、幂等、可续传）。
锁文件独立（`data/.gdrive_archive_sync.lock`），**不影响**每天 17:30/23:30 的行情增量备份。

## 退出码与状态

| 码 | 含义 |
|---|---|
| 0 | 成功（或 `skipped=lock_busy` 跳过） |
| 3 | 依赖缺失（jq / python3 / rclone） |
| 4 | 授权预检失败（`rclone about gdrive:` 不通过） |
| 5 | 部分源失败（其余已传） |

`last_success_at` 只有整轮 exit=0 且未跳过才推进。

**状态文件在开跑时就会写一次**（`exit_code: null` = 「已开始、未结束」），并在
收到 SIGTERM/SIGINT/SIGHUP 时再写一次（`exit_code: 143` + `error_tail`）。一次性
备份**不挂 cron**（没有自愈兜底），中断后若不留痕迹，运维无法区分「从没跑」
与「死在 1%」——2026-10-20 那轮就是这样丢的（410/31 086 个文件后被 SIGTERM，
既没有状态文件也没有失败原因）。

`error_tail` 第一来源是**脚本自己的失败记录**（如 `copy_failed:<源> rc=143`），
其次才是日志里的 `error/failed/失败` 行——被 SIGTERM 的 rclone 不会写 `ERROR :`
行，只 grep 日志会得到空串。

## 一致性（为什么不会备份到半个文件）

写入方（`mootdx_service`、`trade_calendar`）都是 **tmp 文件 + rename 原子替换**
（`part.tmp` → `part.parquet`），因此 `part.parquet` 永远不会是"写了一半"的状态；
备份脚本另外排除 `*.tmp`/`*.tmp.parquet`/`*.part`/`*.crdownload`/`*.lock`
（写入中断的残留不会进单向远端——远端只增不减，进去就永远留在那）。

## 恢复

```bash
# 单个目录
~/.local/bin/rclone copy gdrive:tickflow-stockdata/archive/A股分钟数据 ./A股分钟数据
# 单个文件
~/.local/bin/rclone copyto gdrive:tickflow-stockdata/archive/A股_分时数据_指数/指数列表_沪深京.csv ./指数列表_沪深京.csv
# 校验（只读）
~/.local/bin/rclone check gdrive:tickflow-stockdata/archive/A股分钟数据 ./A股分钟数据
```

## 耗时预期

Google Drive 对 rclone 共享 client_id 有重度限流（实测 250 KiB/s～2.5 MB/s 波动），146 G 按此速率
**约 1～3 天**。上传是后台 `setsid` 起的，会话断开也继续；中断后重跑即续传（size+mtime 去重）。
