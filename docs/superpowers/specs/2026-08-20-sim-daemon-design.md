# 模拟盘进程守护（SimDaemon）设计

日期：2026-08-20
分支：`feature/sim-daemon`（从 `custom-main` 新建）
状态：已批准

## 背景与目标

量化模拟盘账户由主后端 `account_start()` 派生子进程 `scripts/run_quant_sim.py <account_id>` 运行。
目前若该子进程崩溃（异常、OOM 硬杀、主后端/整机重启），账户不会被自动拉起：

- 异常崩溃：runner 捕获异常 → 置 `failed` 后退出（进程有机会留痕）。
- 硬杀（SIGKILL/OOM/段错误）：status 停留 `running`、pid 死亡，进程无机会留痕。
- 主后端/整机重启：旧 pid 失效，main.py 目前把 `running`+死 pid 的账户直接置为 `paused`（不重拉）。

目标：给所有量化模拟盘加一个守护，运行中的账户进程挂了自动拉起；已恢复（runner 崩溃续跑
路径）可无缝接续。

## 触发范围（守护条件）

**仅 `status=running` 且 pid 已死 且 无 `{aid}.pause` 文件**的账户才自动拉起。

- `failed` 状态（异常崩溃已置 failed）**不碰**，由用户手动处理——天然避免带病策略无限重启。
- `created` / `paused` 不碰。
- pause 文件存在 = 有意停止（暂停/reset/delete 过渡期）→ 不拉。

## 架构与组件

### 新增 `SimDaemon` — `backend/app/quant/simulate/daemon.py`

```
SimDaemon
 ├─ start()          → 立刻首扫 + 启动守护线程（daemon=True，命名 sim-daemon）
 ├─ stop()           → set stop event + join
 ├─ _watch()         → while not stop: _sweep(); sleep(10)
 └─ _sweep()         → 遍历 list_sim_accounts()，对 status=running 的账户做存活判定
      ├─ _alive(aid, pid) → /proc/{pid} 存在 且 cmdline 含 "run_quant_sim.py {account_id}"
      └─ 判定死了 → 确认仍 running 且无 {aid}.pause → account_ensure_running(aid)
```

存活判定用 cmdline 匹配防 pid 复用：`/proc/{pid}` 存在不代表还是本账户进程，必须校验
`/proc/{pid}/cmdline` 含 `run_quant_sim.py {account_id}`。读取 cmdline 失败（权限/竞态）→
保守按「死」处理，可重扫一次确认后尝试重启。

单线程 sweep（无并发 sweep）；DB 读写走 quant.db 既有 `get_conn`。

### `service.py` 新增 `account_ensure_running(aid)`

- 复读账户，仅当 `status=running` 且无 pause 文件时执行（防御二次判定竞态）。
- **绕过 `account_start` 的幂等 early-return**（它遇 running 直接返回，无法重拉）——
  独立实现 spawn：清 pause 文件、置 `running` + `started_at`、
  `subprocess.Popen([sys.executable, run_quant_sim.py, aid], start_new_session=True)`、
  pid 落库。
- 重启前写一条 sim_logs：`检测到进程退出，自动重启`。
- Popen 抛异常 → 记 error 日志，本轮跳过，下轮再试；不置 failed（保留 running 语义，daemon 持续重试）。

### `main.py` 接线

- lifespan 启动处：把现有「running+死 pid 置 paused」恢复块替换为启动 `SimDaemon`
  （首扫即刻拉起，无需等轮询）。
- shutdown 处 `daemon.stop()`。
- env 开关 `SIM_DAEMON_ENABLED`（默认开，`0/false` 关闭，对齐 `STOCKDATA_ENABLED` 模式）。

### 防误拉竞态辅助修改

- `account_reset` / `account_delete` 改为**先写 pause 文件再 kill**（现仅 kill 失败才写），
  堵住「kill 后 DB 更新前 daemon 误拉起」的窗口。pause 文件由下次
  `account_start`/`account_ensure_running` 清除，语义不变。

## 数据流与生命周期

**正常生命周期**
```
UI 点「启动」 → account_start(aid) → status=running + pid 落库 + 子进程运行
   ↓
子进程运行中（交易时段逐分钟驱动 / 非交易时段空转）
   ↓
UI 点「暂停」→ account_pause(aid) → 写 pause 文件 + status=paused → 子进程下一轮退出
```

**守护介入场景**
```
场景A 硬杀（OOM/SIGKILL/段错误）：
  status 停留 running、pid 死 → daemon 判定死 → account_ensure_running → 重启
  → runner 走崩溃续跑路径：_restore_portfolio 恢复持仓、
    _replay_partial_day/_replay_history 补跑缺失分钟 → 无缝续跑

场景B 主后端/整机重启：
  status=running + 旧 pid 死 → daemon 首扫即刻全部拉起（替代原先"置 paused"）
  主后端重启但子进程存活：子进程 start_new_session 独立存活，pid 仍有效 →
  daemon 判定存活，不重复拉起（无重复进程）
```

**为何安全不重单**：`_strategy_tick` 有 `last_bar` 去重 + `_seed_fired_before` 预填 fired，
同 bar 不重复触发；`sim_state` 落库为崩溃恢复提供唯一事实源。重启即续跑。

**人工操作不被覆盖**：pause 文件存在 = 有意停止 → daemon 跳过；reset/delete 先写 pause
文件 → 过渡期 daemon 跳过；`failed`（异常崩溃已置 failed）→ daemon 不碰。

## 错误处理与边界

| 场景 | 处理 |
|------|------|
| pid 复用 | cmdline 校验不匹配视为死 |
| cmdline 读取失败 | 保守按死处理，重扫确认后重启 |
| Popen 抛异常 | 记 error 日志，本轮跳过，下轮重试；不置 failed |
| 重启后再次秒退（策略编译失败等） | runner 置 `failed` → daemon 不碰 → 自然防崩溃循环 |
| 连续硬杀循环（极端 OOM） | 不做复杂退避，靠 sim_logs 留痕 |
| 判定死之后用户点暂停 | account_ensure_running 复读 status + 检查 pause，非 running/有 pause 放弃 |
| reset/delete 过渡窗口 | 这两处改为先写 pause 文件再 kill，daemon 见 pause 跳过 |
| daemon 与 account_start 同时 spawn | 仅 daemon 有 force 路径；account_start 幂等不重复；sweep 单线程 |

## 测试方案

单元测试 `backend/tests/quant/test_sim_daemon.py`（仿 `tests/test_stockdata_guardian.py`，
隔离真实 store——参考既有测试的注册隔离模式）：

1. **`_alive` 判定**：/proc 存在 + cmdline 匹配 → True；pid 不存在 → False；
   pid 被复用（cmdline 不匹配 run_quant_sim.py {aid}）→ False。
2. **`_sweep` 决策**：running + 死 pid + 无 pause → 调 account_ensure_running（patch spawn，
   断言被调用）；running + 死 pid + 有 pause → 不拉；created/paused/failed → 不拉；
   running + pid 存活 → 不拉。
3. **`account_ensure_running`**：复读非 running → 不 spawn；running + 无 pause → spawn
   （patch subprocess.Popen，断言 args/start_new_session）、pid 落库、写 sim_logs。
4. **service 竞态防护**：account_reset / account_delete 先写 pause 再 kill（断言顺序）。
5. **main.py 接线**（可选集成）：`SIM_DAEMON_ENABLED` env 开关读取。

验证：`cd backend && uv run --extra dev pytest tests/quant/test_sim_daemon.py -q` +
`uv run --extra dev ruff check app` + `uv run --extra dev mypy app`。
