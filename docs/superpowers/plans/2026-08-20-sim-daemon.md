# 模拟盘进程守护（SimDaemon）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给所有量化模拟盘加一个守护，运行中账户（`status=running`）进程挂了自动拉起，崩溃续跑无缝接续。

**Architecture:** 新增 `SimDaemon` 类（`backend/app/quant/simulate/daemon.py`），由主后端 lifespan 启动后台守护线程，周期性 `_sweep()` 扫描 `sim_accounts`，对 `status=running` 且 pid 已死且无 pause 文件的账户调用新增的 `service.account_ensure_running()` 重拉子进程。存活判定用 `/proc/{pid}/cmdline` 校验防 pid 复用。

**Tech Stack:** Python 3 / FastAPI / sqlite3（quant.db）/ subprocess。测试用 pytest + monkeypatch。

## Global Constraints

- 触发条件**仅**：`status=running` 且 pid 已死 且 无 `{aid}.pause` 文件。
- `failed`/`created`/`paused` 状态**不碰**；有 pause 文件**不碰**。
- 存活判定必须校验 `/proc/{pid}/cmdline` 含 `run_quant_sim.py {account_id}`（防 pid 复用）。
- DB 读写一律走 `app.quant.db` 既有函数（`get_conn`/`list_sim_accounts`/`get_sim_account`/`update_sim_account`/`insert_sim_log`）。
- 测试跑在 `backend/` 目录：`uv run --extra dev pytest ...`（不要裸跑 `uv run pytest`）。
- line-length 100；`uv run --extra dev ruff check app`；`uv run --extra dev mypy app`。
- `account_reset`/`account_delete` 必须改为**先写 pause 文件再 kill**（防误拉竞态）。

---

### Task 1: `service.py` 新增 `account_ensure_running`

**Files:**
- Modify: `backend/app/quant/service.py`（在 `account_start` 之后新增函数；`_script` 已存在）
- Test: `backend/tests/quant/test_sim_daemon.py`（新建，本任务先只加 `account_ensure_running` 相关用例）

**Interfaces:**
- Consumes: `db.get_sim_account(aid)`, `db.update_sim_account(aid, ...)`, `db.insert_sim_log(aid, ts, level, msg)`, `CONFIG.runtime_dir`, `_script("run_quant_sim.py")`, `subprocess.Popen`, `datetime`
- Produces: `service.account_ensure_running(aid: str) -> None` — 复读账户，仅当 `status=running` 且无 `{aid}.pause` 时 spawn 子进程；清 pause、置 `running`+`started_at`、pid 落库、写一条 sim_logs；Popen 异常记 error 日志不 raise。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_sim_daemon.py` 新建（内容含后续任务会用到的 fixture 与用例，本任务先落 `account_ensure_running` 的）：

```python
"""模拟盘进程守护（SimDaemon）测试。隔离真实 store（tmp_path + CONFIG 覆盖）。"""
from __future__ import annotations

import types

import pytest

from app.quant import db, service
from app.quant.config import CONFIG


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path / "strategies"))
    db.init_db(str(db_path))
    return tmp_path


class _FakePopen:
    def __init__(self, args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.pid = 4242


def test_account_ensure_running_spawns_when_running_no_pause(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args[0][-1] == "a1"
    assert kwargs["start_new_session"] is True
    acct = db.get_sim_account("a1")
    assert acct["pid"] == 4242
    assert acct["status"] == "running"
    logs = db.get_sim_logs("a1")
    assert any("自动重启" in m["message"] for m in logs)


def test_account_ensure_running_skips_when_not_running(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "paused")
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert calls == []
    assert db.get_sim_account("a1")["pid"] is None


def test_account_ensure_running_skips_when_pause_file(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    pause = tmp_quant / "quant_sim" / "a1.pause"
    pause.parent.mkdir(parents=True, exist_ok=True)
    pause.write_text("")
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakePopen(*a, **k))
    service.account_ensure_running("a1")
    assert calls == []


def test_account_ensure_running_logs_error_on_popen_failure(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    monkeypatch.setattr(service.subprocess, "Popen", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    service.account_ensure_running("a1")  # 不应 raise
    logs = db.get_sim_logs("a1")
    assert any(m["level"] == "error" for m in logs)
```

- [ ] **Step 2: 运行测试验证失败**

Run: `uv run --extra dev pytest tests/quant/test_sim_daemon.py -q`
Expected: FAIL（`ImportError: cannot import name 'account_ensure_running'` 或 `AttributeError`）。

- [ ] **Step 3: 实现 `account_ensure_running`**

在 `backend/app/quant/service.py` 中，紧跟 `account_start`（第 140 行 `account_start` 函数结束后）新增：

```python
def account_ensure_running(aid: str) -> None:
    """守护自动拉起：仅当账户 status=running 且无 pause 文件时 spawn 子进程。

    与 account_start 不同——account_start 遇 running 幂等直接返回，无法重拉
    崩溃的账户。守护用此入口独立重拉：清 pause、置 running、pid 落库、留日志。
    """
    acct = db.get_sim_account(aid)
    if not acct or acct.get("status") != "running":
        return
    pause = os.path.join(CONFIG.runtime_dir, f"{aid}.pause")
    if os.path.exists(pause):
        return
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    db.update_sim_account(aid, started_at=datetime.datetime.now().isoformat())
    try:
        proc = subprocess.Popen(
            [sys.executable, _script("run_quant_sim.py"), aid],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:  # noqa: BLE001
        db.insert_sim_log(aid, str(datetime.datetime.now()), "error",
                          f"守护自动重启失败: {e}")
        return
    db.update_sim_account(aid, pid=getattr(proc, "pid", None))
    db.insert_sim_log(aid, str(datetime.datetime.now()), "warn",
                      "检测到进程退出，自动重启")
```

- [ ] **Step 4: 运行测试验证通过**

Run: `uv run --extra dev pytest tests/quant/test_sim_daemon.py -q`
Expected: PASS（4 passed）。

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/service.py backend/tests/quant/test_sim_daemon.py
git commit -m "feat(sim): service.account_ensure_running 守护自动拉起入口"
```

---

### Task 2: 防误拉竞态 — `account_reset`/`account_delete` 先写 pause 再 kill

**Files:**
- Modify: `backend/app/quant/service.py:149-170`（`account_reset` 与 `account_delete`）
- Test: `backend/tests/quant/test_sim_daemon.py`（追加用例）

**Interfaces:**
- Consumes: `service.account_reset(aid)`, `service.account_delete(aid)`, `db.get_sim_account`
- Produces: 行为变化——两函数无条件先写 `{aid}.pause` 再 kill 进程组；pause 文件由后续 `account_start`/`account_ensure_running` 清除。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_sim_daemon.py` 追加：

```python
def test_account_reset_writes_pause_even_when_kill_succeeds(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=9999)
    monkeypatch.setattr(service, "kill_process_group", lambda pid: True)
    monkeypatch.setattr(service, "_SIM_CHILD_TABLES", ())
    service.account_reset("a1")
    import os as _os
    assert _os.path.exists(tmp_quant / "quant_sim" / "a1.pause")
```

- [ ] **Step 2: 运行测试验证失败**

Run: `uv run --extra dev pytest tests/quant/test_sim_daemon.py::test_account_reset_writes_pause_even_when_kill_succeeds -q`
Expected: FAIL（当前代码仅在 `kill_process_group` 返回 False 时才写 pause；本例 kill 返回 True 不会写 pause，断言 pause 存在必然失败）。

- [ ] **Step 3: 实现**

修改 `backend/app/quant/service.py` 的 `account_reset`（第 149-161 行）与 `account_delete`（第 163-169 行），把「仅 kill 失败才写 pause」改为「无条件先写 pause 再 kill」：

`account_reset` 新实现：

```python
def account_reset(aid: str) -> None:
    acct = db.get_sim_account(aid) or {}
    # M4：先停活进程再清库，否则旧进程继续循环会把已删状态"复活"。
    # 先写 pause 再 kill：堵住 kill 后 DB 更新前 daemon 误拉起的窗口
    # （daemon 见 pause 文件会跳过）。pause 由下次 account_start 清除。
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w"):
        pass
    kill_process_group(acct.get("pid"))
    db.update_sim_account(aid, status="created", pid=None)
    with db.get_conn() as c:
        for t in _SIM_CHILD_TABLES:
            c.execute(f"DELETE FROM {t} WHERE account_id=?", (aid,))
```

`account_delete` 新实现：

```python
def account_delete(aid: str) -> None:
    acct = db.get_sim_account(aid) or {}
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    with open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w"):
        pass
    kill_process_group(acct.get("pid"))
    db.delete_sim_account(aid)
```

- [ ] **Step 4: 运行测试验证通过**

Run: `uv run --extra dev pytest tests/quant/test_sim_daemon.py -q`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/service.py backend/tests/quant/test_sim_daemon.py
git commit -m "fix(sim): reset/delete 先写 pause 再 kill，堵 daemon 误拉竞态"
```

---

### Task 3: 新增 `SimDaemon` 类（存活判定 + sweep 决策）

**Files:**
- Create: `backend/app/quant/simulate/daemon.py`
- Test: `backend/tests/quant/test_sim_daemon.py`（追加 `_alive` 与 `_sweep` 用例）

**Interfaces:**
- Consumes: `service.account_ensure_running(aid)`, `db.list_sim_accounts()`, `CONFIG.runtime_dir`, `os.path`, `time`, `threading`
- Produces:
  - `daemon._alive(aid: str, pid) -> bool` — `/proc/{pid}` 存在且 `/proc/{pid}/cmdline` 含 `run_quant_sim.py {aid}` 才 True。
  - `SimDaemon(poll_interval: float = 10.0)`，方法 `start()`, `stop()`, `_sweep()`；`_sweep()` 遍历 running 账户，死 pid 且无 pause → `service.account_ensure_running(aid)`。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_sim_daemon.py` 追加：

```python
import os as _os

from app.quant.simulate import daemon


def test_alive_true_when_proc_and_cmdline_match(tmp_quant, monkeypatch):
    pid = _os.getpid()  # 当前进程存在，但 cmdline 不含 run_quant_sim
    assert daemon._alive("a1", pid) is False


def test_alive_false_when_pid_missing(tmp_quant, monkeypatch):
    assert daemon._alive("a1", 999999) is False


def test_alive_false_when_cmdline_mismatch(tmp_quant, monkeypatch):
    # 模拟 pid 被复用为其它进程：cmdline 不匹配 run_quant_sim.py {aid}
    pid = _os.getpid()
    monkeypatch.setattr(daemon, "_read_cmdline", lambda p: "python\x00other.py")
    assert daemon._alive("a1", pid) is False


def test_alive_true_when_cmdline_matches(tmp_quant, monkeypatch):
    pid = _os.getpid()
    monkeypatch.setattr(daemon, "_read_cmdline",
                        lambda p: "/usr/bin/python\x00/path/run_quant_sim.py\x00a1")
    monkeypatch.setattr(daemon.os.path, "exists", lambda p: True)
    assert daemon._alive("a1", pid) is True


def test_sweep_restarts_dead_running_no_pause(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    db.insert_sim_account("a2", "acc2", 100000.0, 0.03, "paused")
    db.insert_sim_account("a3", "acc3", 100000.0, 0.03, "failed")
    calls = []
    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: False)  # 全部判死
    monkeypatch.setattr(daemon.service, "account_ensure_running",
                        lambda aid: calls.append(aid))
    d = daemon.SimDaemon()
    d._sweep()
    assert calls == ["a1"]  # 仅 running 账户被拉起


def test_sweep_skips_when_pause_file(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    pause = tmp_quant / "quant_sim" / "a1.pause"
    pause.parent.mkdir(parents=True, exist_ok=True)
    pause.write_text("")
    calls = []
    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: False)
    monkeypatch.setattr(daemon.service, "account_ensure_running",
                        lambda aid: calls.append(aid))
    d = daemon.SimDaemon()
    d._sweep()
    assert calls == []


def test_sweep_skips_alive_process(tmp_quant, monkeypatch):
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "running")
    db.update_sim_account("a1", pid=12345)
    calls = []
    monkeypatch.setattr(daemon, "_alive", lambda aid, pid: True)
    monkeypatch.setattr(daemon.service, "account_ensure_running",
                        lambda aid: calls.append(aid))
    d = daemon.SimDaemon()
    d._sweep()
    assert calls == []
```

- [ ] **Step 2: 运行测试验证失败**

Run: `uv run --extra dev pytest tests/quant/test_sim_daemon.py -q`
Expected: FAIL（`ImportError: cannot import name 'daemon'`）。

- [ ] **Step 3: 实现 `daemon.py`**

新建 `backend/app/quant/simulate/daemon.py`：

```python
"""模拟盘进程守护：运行中账户（status=running）子进程挂了自动拉起。

由主后端 lifespan 启动后台守护线程，周期性扫描 sim_accounts。
仅拉起 status=running 且 pid 已死且无 pause 文件的账户（pause=有意停止）。
存活判定用 /proc/{pid}/cmdline 匹配 run_quant_sim.py {account_id} 防 pid 复用。
"""
from __future__ import annotations

import os
import threading
import time

from .. import db
from ..config import CONFIG
from .. import service

logger = logging.getLogger("app.quant.simulate.daemon")


def _read_cmdline(pid) -> str:
    """读取 /proc/{pid}/cmdline（以 \x00 分隔拼接）；失败返回空串。"""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().decode(errors="ignore")
    except OSError:
        return ""


def _alive(aid: str, pid) -> bool:
    """进程是否确为本账户的模拟盘进程（防 pid 复用）。

    /proc/{pid}/cmdline 参数间以 \\x00 分隔，脚本路径可能是绝对路径；
    统一转空白后按 token 匹配：存在 run_quant_sim.py 且 aid 是独立 token。
    """
    if not pid:
        return False
    if not os.path.exists(f"/proc/{pid}"):
        return False
    tokens = _read_cmdline(pid).replace("\x00", " ").split()
    return any(t.endswith("run_quant_sim.py") for t in tokens) and aid in tokens


class SimDaemon:
    """守护线程：周期扫描 running 账户，死 pid 自动拉起。"""

    def __init__(self, poll_interval: float = 10.0):
        self._poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _should_restart(self, acct: dict) -> bool:
        aid = acct["id"]
        if acct.get("status") != "running":
            return False
        if _alive(aid, acct.get("pid")):
            return False
        if os.path.exists(os.path.join(CONFIG.runtime_dir, f"{aid}.pause")):
            return False
        return True

    def _sweep(self) -> None:
        try:
            for acct in db.list_sim_accounts():
                if self._should_restart(acct):
                    logger.warning("sim account %s dead, auto restarting", acct["id"][:8])
                    service.account_ensure_running(acct["id"])
        except Exception:  # noqa: BLE001
            logger.exception("[sim-daemon] sweep 异常，下轮重试")

    def _watch(self) -> None:
        self._sweep()  # 首扫
        while not self._stop.wait(self._poll_interval):
            self._sweep()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._watch, name="sim-daemon",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
```

注意：文件顶部需加 `import logging` 并定义 `logger`（`_sweep` 用到）。完整头部：

```python
from __future__ import annotations

import logging
import os
import threading
import time

from .. import db
from ..config import CONFIG
from .. import service

logger = logging.getLogger("app.quant.simulate.daemon")
```

- [ ] **Step 4: 运行测试验证通过**

Run: `uv run --extra dev pytest tests/quant/test_sim_daemon.py -q`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/simulate/daemon.py backend/tests/quant/test_sim_daemon.py
git commit -m "feat(sim): SimDaemon 存活判定 + sweep 自动拉起决策"
```

---

### Task 4: `main.py` 接线 SimDaemon（含 env 开关）

**Files:**
- Modify: `backend/app/main.py:261-274`（替换「running+死 pid 置 paused」恢复块）与 shutdown 段（`yield` 后）
- Test: 无（env 开关读取属可选集成，用 ruff/mypy 验证）

**Interfaces:**
- Consumes: `SimDaemon`, `os.getenv`
- Produces: 主后端启动时若 `SIM_DAEMON_ENABLED` 非 `0/false` 则启动 `SimDaemon`；shutdown 时 `stop()`。

- [ ] **Step 1: 实现**

替换 `backend/app/main.py` 第 261-274 行「模拟盘恢复」块为：

```python
    # 模拟盘守护：运行中账户进程挂了自动拉起（首扫即刻拉起，替代原"置 paused"恢复）
    try:
        if os.getenv("SIM_DAEMON_ENABLED", "true").lower() not in ("0", "false", "no"):
            from app.quant.simulate.daemon import SimDaemon
            sim_daemon = SimDaemon()
            sim_daemon.start()
            app.state.sim_daemon = sim_daemon
            logger.info("sim daemon started")
    except Exception:  # noqa: BLE001
        logger.warning("sim daemon not started: %s", exc_info=True)
```

在 shutdown 段（`sg = getattr(app.state, "stockdata_guardian", None); if sg: sg.stop()` 之后）新增：

```python
    sd = getattr(app.state, "sim_daemon", None)
    if sd:
        sd.stop()
```

- [ ] **Step 2: 验证 lint / 类型 / 导入**

Run: `uv run --extra dev ruff check app/quant/service.py app/quant/simulate/daemon.py app/main.py`
Expected: 无错误（若 import 顺序问题，`ruff check --fix app` 自动整理）。

Run: `uv run --extra dev mypy app/quant/service.py app/quant/simulate/daemon.py`
Expected: 通过（daemon.py 中 `import logging` 等需完整）。

Run: `uv run --extra dev pytest tests/quant/test_sim_daemon.py tests/quant/test_api_quant.py tests/quant/test_runner_strategy.py -q`
Expected: PASS（回归：service 改动不影响既有模拟盘用例）。

- [ ] **Step 3: Commit**

```bash
git add backend/app/main.py
git commit -m "feat(sim): main.py 接线 SimDaemon 守护（SIM_DAEMON_ENABLED 开关）"
```

---

### Task 5: 全量回归验证

**Files:**
- Test: 全量 quant 测试 + lint + mypy

**Interfaces:**
- Consumes: 前序全部改动

- [ ] **Step 1: 全量回归**

Run: `uv run --extra dev pytest tests/quant -q`
Expected: 全部 PASS（或与改动无关的既有失败保持原样，记录之）。

- [ ] **Step 2: lint + 类型**

Run: `uv run --extra dev ruff check app`
Expected: 无错误。

Run: `uv run --extra dev mypy app`
Expected: 无错误（若 mypy 对 daemon.py `_read_cmdline` 返回等有异议，修正类型标注）。

- [ ] **Step 3: Commit（如有残留修改）**

```bash
git add -A
git commit -m "chore: 模拟盘守护全量回归"
```

---

## Self-Review 说明

- **Spec 覆盖**：触发范围（Task 3 `_should_restart`）、架构（Task 1/3/4）、防误拉竞态（Task 2）、错误处理（Task 1 Popen 异常、Task 3 sweep 异常）、测试方案（Task 1-3）。全部覆盖。
- **无占位符**：所有步骤含完整代码与命令。
- **类型一致性**：`account_ensure_running(aid: str) -> None` 全计划一致；`_alive(aid, pid)`、`SimDaemon(poll_interval=10.0)`、`start()/stop()/_sweep()` 在 Task 3/4 中一致。
