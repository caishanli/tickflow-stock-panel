# 量化模拟盘钉钉推送功能设计

## 概述

在量化模拟盘的自定义策略中增加钉钉消息推送能力。策略代码通过 `log.notify(msg)` 触发通知，消息以 Markdown 格式推送到钉钉自定义机器人。推送功能按账户粒度开关，默认关闭。

## 需求

- 策略代码调用 `log.notify("消息内容")` 触发推送
- 钉钉 webhook URL 全局配置（设置页），所有策略共用
- 模拟盘账户详情页有开关按钮，控制该账户是否启用钉钉推送
- 消息格式为 Markdown
- 仅量化模拟盘范围，不涉及 tickflow 主应用监控

## 架构

```
策略代码: log.notify("买入159985")
    ↓
LogProxy.notify() -> _emit_sink("notify", msg)
    ↓
runner.py log_sink 回调
    ├─ level == "notify" 且账户开启了钉钉 -> 异步发送到钉钉 webhook
    └─ 其他 level -> 写入 sim_logs（现有逻辑不变）
    ↓
quant/notify.py: send_dingtalk(webhook_url, secret, title, text)
    ↓
钉钉机器人 API (Markdown 消息)
```

关键设计：
- `log.notify()` 在钉钉未开启时退化为 `log.info()`（写入日志但不推送），策略代码不需要判断
- 钉钉发送异步执行（ThreadPoolExecutor），不阻塞策略主循环
- 所有代码在 `backend/app/quant/` 下，不碰主应用的 `webhook_adapter.py`

## 后端组件

### 新建文件

**`backend/app/quant/notify.py`** - 钉钉发送逻辑

```python
def send_dingtalk(webhook_url: str, secret: str, title: str, text: str) -> bool:
    """发送 Markdown 消息到钉钉自定义机器人。"""
```

- 支持钉钉加签（HMAC-SHA256），和飞书 webhook 同模式
- 发送失败只记 warning 日志，不抛异常（不能影响策略运行）
- 用 `requests.post`，超时 5 秒

### 修改文件

**`backend/app/quant/jqengine/engine/jq/api.py`** - LogProxy 加 `notify()` 方法

```python
def notify(self, msg):
    """推送通知（钉钉等）。未开启时退化为 log.info。"""
    self.info(msg)           # 始终写入 sim_logs
    _emit_sink("notify", msg)  # 额外触发通知 sink
```

**`backend/app/quant/simulate/runner.py`** - log_sink 回调加钉钉分支

- `_emit_log` 内部判断 `level == "notify"` 且账户开启了钉钉推送时，提交到线程池异步发送
- 补跑期间设 `_state["replaying"] = True`，sink 回调检查此标记，补跑时跳过钉钉推送

**`backend/app/quant/db.py`** - sim_accounts 表加字段

```sql
ALTER TABLE sim_accounts ADD COLUMN dingtalk_enabled INTEGER DEFAULT 0;
```

新增 `quant_settings` 表（key-value），存储钉钉 webhook URL 和 secret：

```sql
CREATE TABLE IF NOT EXISTS quant_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
```

相关读写函数：`get_quant_setting(key)` / `set_quant_setting(key, value)`

**`backend/app/quant/api/quant.py`** - 新增 API

- `GET /api/quant/settings/dingtalk` - 获取钉钉 webhook 配置
- `PUT /api/quant/settings/dingtalk` - 保存 webhook URL + secret
- `PUT /api/quant/sim/accounts/{aid}/dingtalk` - 开关推送（body: `{"enabled": true}`）
- `POST /api/quant/settings/dingtalk/test` - 发送测试消息

## 前端组件

### 模拟盘账户详情页（`frontend/src/quant/pages/QuantSim.tsx`）

顶栏控制区加钉钉开关按钮：

```
[返回列表] wufuv5.2-10000  7deafa31  运行中  ··· [钉钉推送] [启动] [暂停] [重置]
```

- 开关点击调 `PUT /api/quant/sim/accounts/{aid}/dingtalk`
- 状态从 `sim_accounts` 的 `dingtalk_enabled` 字段读取
- 开启时按钮高亮（蓝色），关闭时灰色

### 量化设置页

加一个"钉钉推送配置"卡片：

- Webhook URL 输入框
- 加签密钥输入框（可选，标注"如机器人设置了加签则填写"）
- 测试发送按钮（调 API 发一条测试消息）

### API 封装（`frontend/src/quant/api.ts`）

```typescript
toggleDingtalk(aid: string, enabled: boolean): Promise<void>
getDingtalkConfig(): Promise<{ webhook_url: string; secret: string }>
saveDingtalkConfig(webhookUrl: string, secret: string): Promise<void>
testDingtalk(): Promise<{ success: boolean; message: string }>
```

## 数据流与异常处理

### 完整调用链

```
策略: log.notify("买入159985, 数量4600, 价格2.132")
  ↓
LogProxy.notify()
  ├─ self.info(msg)  -> 写入 sim_logs（和普通日志一样可查）
  └─ _emit_sink("notify", msg)
       ↓
runner._emit_log(account_id, "notify", msg)
  ├─ db.insert_sim_log(...)  -> 日志落库（不变）
  └─ if dingtalk_enabled and not replaying:
       _executor.submit(_send_dingtalk_async, aid, msg)  -> 线程池异步
            ↓
         notify.send_dingtalk(webhook_url, secret, title, text)
            ├─ 成功: return True
            └─ 失败: log.warning("钉钉推送失败: ...")  -> 不抛异常
```

### 钉钉消息格式（Markdown）

```markdown
### 模拟盘通知 [wufuv5.2-10000]

买入159985, 数量4600, 价格2.132

> 时间: 2026-07-25 13:10:00  
> 账户: 7deafa31
```

### 异常处理原则

- 钉钉发送失败不影响策略执行（fire-and-forget）
- webhook URL 未配置时跳过发送，记 debug 日志
- 线程池大小 2（通知频率低，不需要大池）
- 发送超时 5 秒，避免线程堆积

### 补跑期间的处理

- 补跑历史数据时 `log.notify()` 照常写入 sim_logs
- 但钉钉推送跳过（避免补跑产生大量历史通知刷屏）
- 判断方式：`_replay_history` 期间设标记，sink 回调检查此标记

## 文件清单

| 操作 | 文件 | 说明 |
|------|------|------|
| 新建 | `backend/app/quant/notify.py` | 钉钉发送逻辑 |
| 修改 | `backend/app/quant/jqengine/engine/jq/api.py` | LogProxy 加 notify() |
| 修改 | `backend/app/quant/simulate/runner.py` | log_sink 加钉钉分支 |
| 修改 | `backend/app/quant/db.py` | 表结构 + 配置读写 |
| 修改 | `backend/app/quant/api/quant.py` | 新增 4 个 API |
| 修改 | `frontend/src/quant/pages/QuantSim.tsx` | 详情页加开关 |
| 修改 | `frontend/src/quant/api.ts` | API 封装 |
| 修改 | `frontend/src/quant/pages/QuantSettings.tsx` | 钉钉配置卡片 |
