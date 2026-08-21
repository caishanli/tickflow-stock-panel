# 模拟盘启动内存守卫设计

日期：2026-08-20

## 背景与问题

模拟盘账户（`sim_accounts`）由两种路径 spawn 子进程 `run_quant_sim.py {aid}`：

- 手动启动：`service.account_start`（UI/API `POST /api/quant/sim/accounts/{aid}/start`）
- 守护自动重拉：`SimDaemon._sweep` → `service.account_ensure_running`（10s 轮询，仅拉
  `status=running` 且 pid 已死且无 `.pause` 文件的账户）

实测机器内存紧张（`/proc/meminfo`：MemTotal ≈ 3.7GB，MemAvailable ≈ 1.8GB，Swap 已在吃）。
此前在启动时 daemon 首扫会把全部 `running` 账户一次性拉起，多个模拟盘子进程全跑
存在把机器打满导致死机的风险。当前没有任何内存门禁。

## 目标与验收

- 任何启动路径（手动/守护）在 spawn 子进程前先检查空闲内存，不足则**拦截**。
- 拦截时：写警告（账户日志 + 系统 logger）；手动启动返回错误让前端提示"内存不足"；
  守护本轮跳过、下轮 10s 自动重试；账户保持原状态（不置 running、不写 pause）。
- 全程不 spawn，保证机器不被拖死。

## 方案（共享内存守卫模块）

### 1. 配置（`backend/app/quant/config.py`）

`QuantConfig` 新增两个字段，env 覆盖：

| 字段 | env | 默认 | 含义 |
|------|-----|------|------|
| `sim_account_mem_mb` | `SIM_ACCOUNT_MEM_MB` | `400.0` | 无活进程样本时的单账户估算兜底（MB） |
| `sim_account_mem_min_mb` | `SIM_ACCOUNT_MEM_MIN_MB` | `300.0` | 实测均值下限（MB），防新进程低 RSS 低估 |

### 2. 新模块 `backend/app/quant/simulate/memory.py`

纯函数、psutil 驱动，便于单测 mock：

- `list_alive_sim_procs() -> list[dict]`：`psutil.process_iter` 遍历，cmdline 含
  `run_quant_sim.py` 的进程，返回 `[{"pid": ..., "rss_mb": ...}]`（cmdline 匹配思路复用
  `simulate/daemon.py::_alive`，不绑定某账户 aid，凡模拟盘进程都算）。
- `estimate_per_account_mb() -> float`：活进程 RSS 均值（MB）；无样本 → `sim_account_mem_mb`；
  有样本 → `max(均值, sim_account_mem_min_mb)`。
- `memory_check(extra: int = 1) -> dict`：
  - `available_mb = psutil.virtual_memory().available / 1024**2`
  - `alive = len(list_alive_sim_procs())`
  - `estimate_mb = estimate_per_account_mb()`
  - `needed_mb = estimate_mb * (alive + extra)`（`extra` = 本次拟新增，默认 1）
  - `ok = available_mb >= needed_mb`
  - 返回 `{"ok", "available_mb", "needed_mb", "estimate_mb", "alive"}`

### 3. `service.py` 两条路径接入（改状态/spawn 之前检查）

- `account_start(aid)`：在幂等/running 判断之后、清 pause / 置 running / Popen **之前**调
  `memory_check(extra=1)`。不通过 → `raise ValueError("内存不足: 可用 %.0fMB < 需要 %.0fMB，"
  "已跳过启动")`；不改状态、不写 pause。
- `account_ensure_running(aid)`：在 status/pause/alive 复核之后、Popen **之前**调
  `memory_check(extra=1)`。不通过 → `db.insert_sim_log(aid, now, "warn", "内存不足: ... 未自动重启，"
  "稍后重试")` + `logger.warning(...)`，return（守护下轮 sweep 再试）。

竞态说明：`psutil.process_iter()` 每次实时读 `/proc`，`Popen` 返回后子进程立即可见，
同一轮 sweep 内逐账户顺序检查时 `alive` 会实时增长，自然限流；极端竞态多拉的量级为毫秒级，
下一轮 sweep 兜底，不做进程预留计数（YAGNI）。

### 4. 反馈链路

- `backend/app/quant/api/quant.py::sim_start`：catch `ValueError` → `HTTPException(400, str(e))`
  （对齐第 268 行 `strategy not found` 的 pattern）。
- `frontend/src/quant/pages/QuantSim.tsx`：`startMut` 加 `onError`，从
  `quant api {status}: {body}` 错误中解析 `{"detail": ...}` 弹 toast（error）。

## 正确性保证

- 只拦启动，不杀已运行的账户：已有 `running` 且进程存活的账户不受影响（不在本轮计算之外，
  其 RSS 计入 `alive` 与均值，新账户的 `needed` 覆盖全部）。
- 守护重试自然收敛：内存释放后下轮 sweep 自动拉起，无需人工介入；内存长期不足则一直跳过并留 warn 日志。
- 口径保守：估算带下限（`sim_account_mem_min_mb`），防止启动初期 RSS 低导致低估多拉。

## 测试

新增 `backend/tests/quant/test_sim_memory.py`（`tmp_quant` fixture 隔离 DB + CONFIG，mock psutil）：

- 无活进程 → `estimate_per_account_mb` 回退默认 `sim_account_mem_mb`
- 有活进程 → `max(均值, sim_account_mem_min_mb)`
- `memory_check`：available < needed 时 `ok=False` 且数值正确；≥ 时 `ok=True`
- `account_start` 内存不足 → raise ValueError、状态不变（不 running）、不写 pause、不 Popen
- `account_start` 内存充足 → 正常 spawn（复用现有 `_FakePopen` pattern）
- `account_ensure_running` 内存不足 → 不 Popen、账户日志含 warn
- `test_sim_daemon.py` 补：`_sweep` 在内存不足时跳过（mock `service.account_ensure_running` 行为，或 mock memory_check）

## 文档

- `AGENTS.md`「stock data 服务」节附近或量化节补 `SIM_ACCOUNT_MEM_MB` / `SIM_ACCOUNT_MEM_MIN_MB` 说明。