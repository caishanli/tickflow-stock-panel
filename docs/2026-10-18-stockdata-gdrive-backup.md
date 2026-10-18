# 行情数据 Google Drive 自动备份

- 日期：2026-10-18
- 设计文档：`docs/superpowers/specs/2026-10-18-stockdata-gdrive-backup-design.md`
- 实现计划：`docs/superpowers/plans/2026-10-18-stockdata-gdrive-backup.md`
- 脚本：`backend/scripts/sync_stockdata_to_gdrive.sh`

## 一句话

本机 `data/` 里的**行情数据**每天 17:30 与 23:30（每天跑，不判断交易日）增量同步到 Google Drive 的
`gdrive:tickflow-stockdata/data`。**单向**（本地 → 云端），脚本永不带 `--delete`，远端只增不减。
首次全量已上传 4.7 GiB / 7520 个文件，之后每次增量约 19 MB。

## 备份范围（白名单）

| 相对路径 | 说明 |
|---|---|
| `kline_minute` | 全市场股票 1m 分钟线（回源主数据） |
| `kline_etf_minute` | ETF 分钟线 |
| `kline_daily` / `kline_etf_daily` / `kline_index_daily` | 股票 / ETF / 指数日线 |
| `kline_daily_enriched` / `kline_etf_enriched` / `kline_index_enriched` | 派生 enriched（可重算，但一并备份做到「取到即用」） |
| `adj_factor` / `adj_factor_etf` | 复权因子 |
| `instruments` / `instruments_etf` / `instruments_index` | 标的表 |
| `financials` / `industries_sw` / `etf_nav` | 财务、申万行业、ETF 净值 |
| `trade_calendar.json` | 权威交易日历 |

**不在范围**：`quant.db*`、`quant_sim/`、`quant_kline/`、`quant_strategies/`、日志、缓存、`backtest_results/`、
`.backtest_matrix_cache/`、`job_store/` 等运行态数据。原因：`quant.db` 是服务运行时活动的 SQLite（含 `-wal`），
热拷贝会拿到不一致状态，不能当备份用。

范围是脚本里的白名单常量（`DIRS` / `FILES`）。**新增数据类型必须显式加进去**，否则不会被备份。

## 手动运行

```bash
cd /home/caisl/tickflow-stock-panel
GDRIVE_TRIGGER=manual backend/scripts/sync_stockdata_to_gdrive.sh; echo "rc=$?"
```

可覆盖的环境变量：

| 变量 | 默认 | 用途 |
|---|---|---|
| `GDRIVE_DATA_DIR` | `<仓库根>/data` | 源数据根 |
| `GDRIVE_REMOTE` | `gdrive:tickflow-stockdata/data` | 远端根（换盘/换账号改这里） |
| `RCLONE_BIN` | `~/.local/bin/rclone` | rclone 二进制 |
| `RCLONE_BWLIMIT` | 空（不限速） | 设了才追加 `--bwlimit`，如 `8M` |
| `RCLONE_TRANSFERS` / `RCLONE_CHECKERS` | `4` / `8` | 并发传输/校验路数。首轮全量建议 `16`/`16`（见下「首轮耗时与调优」） |
| `GDRIVE_MIN_AGE` | `15m` | 跳过刚被写过的文件（防空传半写的 parquet） |
| `GDRIVE_TRIGGER` | `cron` | 只写进状态文件的来源标记 |
| `GDRIVE_LOG` / `GDRIVE_STATUS` / `GDRIVE_LOCK` | `data/gdrive_sync.log` / `data/gdrive_sync_status.json` / `data/.gdrive_sync.lock` | 运行态文件 |

脚本可安全重复运行（增量比对 size+mtime），中断后重跑即续传。

## 首轮耗时与调优（实测 2026-10-18）

本地白名单约 4.15 GiB / 7519 文件（`kline_minute` 565 文件占 4153 MiB；`kline_etf_daily` 5260 个碎文件占 47 MiB）。
Google 侧对上传有明显限流（TCP 层 `rwnd_limited` 80%、连接大段空转、`bytes_sent` 远超实际落盘量 = 重传），
所以**瓶颈是 Drive 侧的延迟/限流，不是上行带宽**（实测本机仍有余量 ~4 MB/s）。据此调优：

| 配置 | 实测速率 |
|---|---|
| `--transfers 4`（旧默认）+ 目录 mtime 同步 | ~94 KiB/s |
| `--transfers 16` + `--no-update-dir-modtime` | **~577 KiB/s** |
| `--transfers 32` | ~338 KiB/s（更差——并发越高被限得越狠） |

两个关键点：

1. **`--no-update-dir-modtime`（脚本已默认带上）**：rclone 会逐目录串行调用 Drive 的目录 mtime 接口，
   `kline_minute` 的 565 个 `date=` 分区就要 ~10 分钟且期间**零字节吞吐**。备份不需要目录 mtime（恢复只关心文件），关掉。
2. **并发度 16 是甜点**：首轮全量用 `RCLONE_TRANSFERS=16 RCLONE_CHECKERS=16`；
   日常增量只有 ~19 MB / 十几文件，用默认 4/8 即可。

按此配置首轮全量预计 2 小时左右。彻底解限流的办法是**换成自己的 OAuth client_id**（见下「已知风险」）。

## 状态与日志

**日志**：`data/gdrive_sync.log`，超过 10 MiB 自动轮转为 `data/gdrive_sync.log.1`（crontab 的 stdout/stderr 也追加到这里）。

**状态**：`data/gdrive_sync_status.json`（原子写，每次覆盖）：

| 字段 | 含义 |
|---|---|
| `last_run_at` | 本轮开始时间 |
| `last_success_at` | **数据最后一次真正上云成功**的时间——只有整轮退出码 0 且未跳过才推进 |
| `exit_code` | 见下表 |
| `trigger` | `cron` / `manual` / `first-full` |
| `skipped` | `lock_busy`（已有实例在跑）或 `null` |
| `dirs_ok` / `dirs_failed` | 成功 / 失败的单元（最大 17 = 16 目录 + 1 文件） |
| `transferred_files` / `transferred_bytes` | 本轮上传量（逐单元累加；解析失败为 `null`） |
| `elapsed_s` / `rclone_version` / `remote` | 耗时、rclone 版本、远端根 |
| `error_tail` | 本轮日志里的错误摘要（≤500 字符） |

**排查第一步永远是这个**：`jq '.' data/gdrive_sync_status.json`。`last_success_at` 长时间不推进 = 备份链断了。

**退出码**：

| 码 | 含义 | 处理 |
|---|---|---|
| 0 | 成功（或 `skipped=lock_busy` 跳过） | — |
| 3 | 依赖缺失（jq / python3 / rclone 二进制） | 按 `error_tail` 装回去 |
| 4 | 授权或连通预检失败（`rclone about gdrive:` 不通过） | 见下方「授权失效」 |
| 5 | 部分单元失败（其余单元已传） | 看 `dirs_failed` 与 `error_tail`，通常下轮自愈 |

## 恢复

先停服务（保证文件一致性），再用 rclone 反向 copy。远端目录与本地**逐字节同构**，所以路径可直接对应：

```bash
# 单个分区（最常用：某天某类数据丢了）
~/.local/bin/rclone copy gdrive:tickflow-stockdata/data/kline_minute/date=2026-10-16 ./data/kline_minute/date=2026-10-16

# 整个数据类型
~/.local/bin/rclone copy gdrive:tickflow-stockdata/data/kline_daily ./data/kline_daily

# 全量恢复（4.7G，慎用；建议先停服务）
~/.local/bin/rclone copy gdrive:tickflow-stockdata/data ./data
```

校验（两边逐文件比对 size+md5，只读不写）：

```bash
~/.local/bin/rclone check gdrive:tickflow-stockdata/data/kline_minute ./data/kline_minute
```

## 定时

写在当前用户 crontab（无需 sudo）：

```
30 17 * * * /home/caisl/tickflow-stock-panel/backend/scripts/sync_stockdata_to_gdrive.sh >> /home/caisl/tickflow-stock-panel/data/gdrive_sync.log 2>&1
30 23 * * * /home/caisl/tickflow-stock-panel/backend/scripts/sync_stockdata_to_gdrive.sh >> /home/caisl/tickflow-stock-panel/data/gdrive_sync.log 2>&1
```

改时间用 `crontab -e`。查看：`crontab -l | grep sync_stockdata`。

为什么放在 crontab 而不放进 stockdata 服务：备份对象跨进程（`kline_*` 是 stockdata 写的，`*_enriched` 是主后端写的），
且备份不该依赖「被备份者」的存活——服务崩溃/`STOCKDATA_ENABLED=0` 时备份仍须工作。详见设计文档 §6。

## 排障

**授权失效（退出码 4）**

```bash
~/.local/bin/rclone about gdrive:      # 吐配额 = 正常；报错 = 授权问题
```

重新授权（本机无桌面浏览器，走 SSH 端口转发或换 token，两种方式见设计文档/提交记录；要点是回调地址固定为
`http://127.0.0.1:53682/`，隧道用 `-L 53682:127.0.0.1:53682`）。

**⚠️ 已知风险：rclone 共享 client_id 将在 2026 年内退役**

`rclone about` 每次都会警告：本 remote 用的是 rclone 官方共享 client_id，**2026 年内会失效**。
本项目按设计没有失败告警，届时备份会**静默停摆**（唯一信号是 `last_success_at` 停滞）。彻底修法：
在 GCP 建自己的 OAuth client（https://rclone.org/drive/#making-your-own-client-id），
把 `client_id` / `client_secret` 写进 `~/.config/rclone/rclone.conf` 后重新授权，约 10 分钟。

另外，2026-10-18 首轮实测显示共享 client_id **当前就已经在被重度限流**（~577 KiB/s，见上节；上行带宽本身有余量），
换自己的 client_id 通常同时解决限速与退役两件事。

**备份没跟上 / 没触发**

1. `crontab -l | grep sync_stockdata` 确认两行在。
2. `jq '.last_run_at,.last_success_at,.skipped' data/gdrive_sync_status.json`：`last_run_at` 不新 = cron 没跑（机器关机？）；`skipped=lock_busy` = 上一轮还没跑完。
3. `tail -50 data/gdrive_sync.log`。

**当天数据要等到什么时候**

17:30 那轮上传当时已落定的部分，23:30 那轮兜底（收盘 15:35 回源在极端情况下会跑到 ~2.2 小时，且 `--min-age 15m`
会跳过 15 分钟内仍在写的文件）。要立刻上云就手动跑一次。

## 已知取舍

- **不做失败告警**、**不做开机漏跑补偿**（下次定时或手动运行即增量追平）。
- 当天数据最晚 23:30 上云。
- 只备份行情，不备份模拟盘交易状态（`quant.db`）——那部分如需备份要单独设计（需 SQLite 一致性快照）。
