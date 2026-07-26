# 量化模拟盘钉钉推送 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在量化模拟盘策略中增加 `log.notify()` 钉钉推送能力，按账户开关，Markdown 格式，异步发送。

**Architecture:** 扩展 LogProxy 加 `notify()` 方法，走现有 `_emit_sink` -> `log_sink` 管道。runner 的 sink 回调判断 level=="notify" 时异步发送到钉钉 webhook。钉钉发送逻辑隔离在 `backend/app/quant/notify.py`。webhook 配置存 `quant_settings` 表，账户级开关存 `sim_accounts.dingtalk_enabled`。

**Tech Stack:** Python (FastAPI, sqlite3, requests), React (TypeScript, Vite)

## Global Constraints

- 所有代码在 `backend/app/quant/` 下，不碰主应用 `backend/app/services/webhook_adapter.py`
- 测试从 `backend/` 目录运行：`uv run --extra dev pytest tests/quant/test_xxx.py -v`
- 前端类型检查：`cd frontend && npx tsc --noEmit`
- 钉钉发送失败不影响策略执行（fire-and-forget）
- 补跑期间跳过钉钉推送（避免刷屏）

---

### Task 1: DB schema + 配置读写

**Files:**
- Modify: `backend/app/quant/db.py`
- Test: `backend/tests/quant/test_db.py`

**Interfaces:**
- Produces: `db.get_quant_setting(key) -> str | None`, `db.set_quant_setting(key, value)`, `sim_accounts.dingtalk_enabled` 列

- [ ] **Step 1: Write failing tests**

Append to `backend/tests/quant/test_db.py`:

```python
def test_quant_settings_kv():
    p = _fresh()
    db.set_quant_setting("dingtalk_webhook_url", "https://oapi.dingtalk.com/robot/send?access_token=xxx")
    db.set_quant_setting("dingtalk_secret", "SECxxx")
    assert db.get_quant_setting("dingtalk_webhook_url") == "https://oapi.dingtalk.com/robot/send?access_token=xxx"
    assert db.get_quant_setting("dingtalk_secret") == "SECxxx"
    assert db.get_quant_setting("nonexistent") is None
    os.unlink(p)


def test_sim_account_dingtalk_enabled():
    p = _fresh()
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "created")
    acct = db.get_sim_account("a1")
    assert acct["dingtalk_enabled"] == 0
    db.update_sim_account("a1", dingtalk_enabled=1)
    acct = db.get_sim_account("a1")
    assert acct["dingtalk_enabled"] == 1
    os.unlink(p)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_db.py::test_quant_settings_kv tests/quant/test_db.py::test_sim_account_dingtalk_enabled -v`
Expected: FAIL

- [ ] **Step 3: Add quant_settings table + dingtalk_enabled column**

In `backend/app/quant/db.py`:
- Add to `_SCHEMA` string (after `sim_logs` table): `CREATE TABLE IF NOT EXISTS quant_settings (key TEXT PRIMARY KEY, value TEXT);`
- In `init_db()`, after `frequency` column migration: add `dingtalk_enabled INTEGER DEFAULT 0` column migration for `sim_accounts`
- Add functions `get_quant_setting(key)` and `set_quant_setting(key, value)` after `list_sim_accounts()`

```python
def get_quant_setting(key: str) -> str | None:
    with get_conn() as c:
        row = c.execute("SELECT value FROM quant_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def set_quant_setting(key: str, value: str) -> None:
    with get_conn() as c:
        c.execute(
            "INSERT INTO quant_settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_db.py::test_quant_settings_kv tests/quant/test_db.py::test_sim_account_dingtalk_enabled -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/db.py backend/tests/quant/test_db.py
git commit -m "feat(quant): add quant_settings table + dingtalk_enabled column"
```

---

### Task 2: 钉钉发送逻辑

**Files:**
- Create: `backend/app/quant/notify.py`
- Test: `backend/tests/quant/test_notify.py`

**Interfaces:**
- Produces: `notify.send_dingtalk(webhook_url: str, secret: str, title: str, text: str) -> bool`

- [ ] **Step 1: Write failing tests**

Create `backend/tests/quant/test_notify.py` with 4 tests:
- `test_send_dingtalk_plain_text` - 无加签发送 Markdown，验证 payload 格式
- `test_send_dingtalk_with_sign` - 带加签，验证 URL 追加 timestamp + sign
- `test_send_dingtalk_failure` - 钉钉返回 errcode!=0 时返回 False
- `test_send_dingtalk_network_error` - 网络异常时不抛异常，返回 False

所有测试 mock `requests.post`，不联网。

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_notify.py -v`
Expected: FAIL with "No module named 'app.quant.notify'"

- [ ] **Step 3: Create notify.py**

Create `backend/app/quant/notify.py` with:
- `_sign(secret, timestamp)` - HMAC-SHA256 加签，base64 + urlencode
- `send_dingtalk(webhook_url, secret, title, text) -> bool` - 发送 Markdown 消息，超时 5 秒，失败不抛异常

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_notify.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/notify.py backend/tests/quant/test_notify.py
git commit -m "feat(quant): add dingtalk notify sender with markdown + signing"
```

---

### Task 3: LogProxy.notify() 方法

**Files:**
- Modify: `backend/app/quant/jqengine/engine/jq/api.py` (LogProxy class, ~line 639)
- Test: `backend/tests/quant/test_log_notify.py`

**Interfaces:**
- Produces: `log.notify(msg)` - 同时触发 `info` sink（写日志）和 `notify` sink（推送）

- [ ] **Step 1: Write failing test**

Create `backend/tests/quant/test_log_notify.py`:
- `test_notify_calls_info_and_sink` - notify() 触发 info + notify 两个 sink level
- `test_notify_without_sink_no_crash` - 无 log_sink 时不崩溃

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL with "LogProxy has no attribute 'notify'"

- [ ] **Step 3: Add notify() to LogProxy**

In `api.py`, after `debug()` method, before `info_format`:
```python
    def notify(self, msg):
        """推送通知（钉钉等）。未开启时退化为 log.info（写日志不推送）。"""
        self.info(msg)
        _emit_sink("notify", msg)
```

- [ ] **Step 4: Run test to verify it passes**

Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/jqengine/engine/jq/api.py backend/tests/quant/test_log_notify.py
git commit -m "feat(quant): add log.notify() to LogProxy"
```

---

### Task 4: Runner 集成钉钉推送

**Files:**
- Modify: `backend/app/quant/simulate/runner.py` (`_emit_log` ~line 129, `_replay_history` ~line 454)
- Test: `backend/tests/quant/test_runner_dingtalk.py`

**Interfaces:**
- Consumes: `db.get_quant_setting`, `db.get_sim_account`, `notify.send_dingtalk`
- Produces: `_send_dingtalk_async(account_id, msg)`, `_emit_log` 在 notify level 时调用它

- [ ] **Step 1: Write failing tests**

Create `backend/tests/quant/test_runner_dingtalk.py`:
- `test_notify_level_triggers_dingtalk_when_enabled` - 账户开启 + 配了 webhook -> 触发发送
- `test_notify_level_skipped_when_disabled` - 账户未开启 -> 不触发
- `test_info_level_never_triggers_dingtalk` - info level -> 不触发

- [ ] **Step 2: Run tests to verify they fail**

Expected: FAIL

- [ ] **Step 3: Add DingTalk dispatch to runner**

In `runner.py`:
1. Add `from concurrent.futures import ThreadPoolExecutor` import
2. Add `_DINGTALK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="dingtalk")`
3. Add `_send_dingtalk_async(account_id, msg)` function:
   - 读取 webhook_url 和 secret from `db.get_quant_setting`
   - 读取账户名 from `db.get_sim_account`
   - 构造 Markdown 消息（标题：模拟盘通知 [账户名]，正文：msg + 时间 + 账户ID）
   - 调用 `notify.send_dingtalk()`
4. 修改 `_emit_log`：当 `level == "notify"` 且账户 `dingtalk_enabled` 且非补跑时，`_DINGTALK_EXECUTOR.submit(_send_dingtalk_async, account_id, msg)`
5. 在 `_replay_history` 开始时设 `_state["replaying"] = True`，结束时设 False（补跑期间跳过钉钉）

- [ ] **Step 4: Run tests to verify they pass**

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_dingtalk.py
git commit -m "feat(quant): integrate dingtalk push into runner log_sink"
```

---

### Task 5: 后端 API

**Files:**
- Modify: `backend/app/quant/api/quant.py`
- Test: `backend/tests/quant/test_api_quant.py`

**Interfaces:**
- Produces: 4 个 API endpoint

- [ ] **Step 1: Write failing tests**

Add to `backend/tests/quant/test_api_quant.py`:
- `test_get_dingtalk_config` - GET 返回 webhook_url + secret
- `test_save_dingtalk_config` - PUT 保存配置
- `test_toggle_dingtalk` - PUT 开关账户钉钉
- `test_test_dingtalk` - POST 发送测试消息（mock send_dingtalk）

- [ ] **Step 2: Run tests to verify they fail**

Expected: FAIL (404)

- [ ] **Step 3: Add 4 API endpoints**

In `quant.py`, add:

```python
@router.get("/settings/dingtalk")
def get_dingtalk_config():
    return {"data": {
        "webhook_url": db.get_quant_setting("dingtalk_webhook_url") or "",
        "secret": db.get_quant_setting("dingtalk_secret") or "",
    }}

@router.put("/settings/dingtalk")
def save_dingtalk_config(body: dict):
    db.set_quant_setting("dingtalk_webhook_url", body.get("webhook_url", ""))
    db.set_quant_setting("dingtalk_secret", body.get("secret", ""))
    return {"data": "ok"}

@router.put("/sim/accounts/{aid}/dingtalk")
def toggle_dingtalk(aid: str, body: dict):
    db.update_sim_account(aid, dingtalk_enabled=1 if body.get("enabled") else 0)
    return {"data": "ok"}

@router.post("/settings/dingtalk/test")
def test_dingtalk():
    from ..notify import send_dingtalk
    url = db.get_quant_setting("dingtalk_webhook_url") or ""
    secret = db.get_quant_setting("dingtalk_secret") or ""
    if not url:
        return {"data": {"success": False, "message": "未配置 webhook URL"}}
    ok = send_dingtalk(url, secret, "测试通知", "## 测试通知\n这是一条来自量化模拟盘的测试消息")
    return {"data": {"success": ok, "message": "发送成功" if ok else "发送失败"}}
```

- [ ] **Step 4: Run tests to verify they pass**

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/api/quant.py backend/tests/quant/test_api_quant.py
git commit -m "feat(quant): add dingtalk config + toggle + test APIs"
```

---

### Task 6: 前端 API 封装 + 钉钉开关 + 配置弹窗

**Files:**
- Modify: `frontend/src/quant/api.ts`
- Modify: `frontend/src/quant/pages/QuantSim.tsx`
- Create: `frontend/src/quant/pages/DingtalkConfigDialog.tsx`

- [ ] **Step 1: Add API wrappers**

In `frontend/src/quant/api.ts`, add:
```typescript
export const toggleDingtalk = (aid: string, enabled: boolean) =>
  j(`/sim/accounts/${aid}/dingtalk`, { method: 'PUT', body: JSON.stringify({ enabled }) })
export const getDingtalkConfig = () => j('/settings/dingtalk')
export const saveDingtalkConfig = (webhookUrl: string, secret: string) =>
  j('/settings/dingtalk', { method: 'PUT', body: JSON.stringify({ webhook_url: webhookUrl, secret }) })
export const testDingtalk = () => j('/settings/dingtalk/test', { method: 'POST' })
```

- [ ] **Step 2: Create DingtalkConfigDialog.tsx**

Simple dialog with:
- Webhook URL input
- Secret input (optional)
- Save button -> `saveDingtalkConfig`
- Test button -> `testDingtalk`, show result

- [ ] **Step 3: Add toggle button to QuantSim.tsx detail page**

In the top bar, add a "钉钉推送" toggle button:
- Read `dingtalk_enabled` from account data
- Click -> `toggleDingtalk(aid, !enabled)` -> invalidate query
- If enabling and no webhook configured, open DingtalkConfigDialog
- Add a small "配置" link to open DingtalkConfigDialog

- [ ] **Step 4: Type check**

Run: `cd frontend && npx tsc --noEmit`
Expected: no errors in quant files

- [ ] **Step 5: Commit**

```bash
git add frontend/src/quant/api.ts frontend/src/quant/pages/QuantSim.tsx frontend/src/quant/pages/DingtalkConfigDialog.tsx
git commit -m "feat(quant): add dingtalk toggle button + config dialog in frontend"
```

---

### Task 7: 策略 API 注入 log.notify

**Files:**
- Modify: `backend/app/quant/jqengine/engine/jq/loader.py` (确认 `log` 已注入，无需额外操作)

- [ ] **Step 1: Verify log is already injected**

Check `loader.py` ns dict - `log` (the LogProxy instance) is already in the strategy namespace. `log.notify()` is automatically available after Task 3 adds the method. No code change needed.

- [ ] **Step 2: Write integration test**

Create `backend/tests/quant/test_strategy_notify.py`:
- Load a minimal strategy that calls `log.notify("test")` in `handle()`
- Verify sim_logs contains a "notify" level entry
- Verify `_send_dingtalk_async` is called when account has dingtalk_enabled=1

- [ ] **Step 3: Run test**

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add backend/tests/quant/test_strategy_notify.py
git commit -m "test(quant): integration test for log.notify() in strategy"
```
