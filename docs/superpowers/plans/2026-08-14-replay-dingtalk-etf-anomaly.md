# 模拟盘补跑钉钉抑制 + 全市场ETF成交额异常自检 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补跑期间不发日常钉钉汇总；策略 3 日 ETF 成交额自检发现异常天时进"异常"标签 + 即时推钉钉，并剔除异常天算阈值。

**Architecture:** 两处小改动在 runner（`_emit_eod_notify` 补跑不推、`_replay_log_sink` 异常即时推），策略自检逻辑抽成可单测的 `_anomalous_etf_days` 辅助函数并接入 `calculate_global_etf_threshold`。实跑策略 `data/quant_strategies/wufu-v5.4-ding.py` 与 fixture `backend/tests/fixtures/wufu_v54/wufu-v5.4-ding.py` 同步更新。

**Tech Stack:** Python 3.11, pandas, pytest（asyncio_mode=auto）, ruff/mypy（backend 目录，`uv run --extra dev`）。

## Global Constraints

- 测试与命令从 `backend/` 目录运行，`uv run --extra dev pytest ...`。
- ruff line-length 100，忽略 E501；中文注释/标点触发的 RUF002/003 与仓库既有风格一致，不新增其它规则违规。
- 异常判定阈值：`< 另两天较大者的 50%`（`<`，恰好 50% 不判）。
- 判定信号：有成交只数 或 总成交额，任一 < 50% 即判异常天。
- 剔除异常天后不足 2 个正常日 → 回落保守阈值 10000000。
- 补跑期间：日常钉钉不推；`🚨【成交额异常】` 前缀的 notify 立即推钉钉。

---

### Task 1: `_emit_eod_notify` 补跑期间不推日常钉钉

**Files:**
- Modify: `backend/app/quant/simulate/runner.py:253-281`（`_emit_eod_notify`）
- Test: `backend/tests/quant/test_runner_strategy.py`

**Interfaces:**
- Consumes: 无（独立函数）。
- Produces: `_emit_eod_notify` 在 `aux["replay_mode"]` 时不再调用 `_dispatch_dingtalk`；实时分支不变。`_replay_day_notifies` 仍被清空。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_runner_strategy.py` 末尾追加（顶部已有 `import datetime`、`runner`、`pytest`；缺 `types`/`SimpleNamespace` 时在文件顶部 import 区补）：

```python
from types import SimpleNamespace

def _eod_fake_ctx():
    return SimpleNamespace(portfolio=SimpleNamespace(positions={}))

def test_eod_notify_suppressed_during_replay(tmp_quant, monkeypatch):
    """补跑期间 _emit_eod_notify 不推钉钉（长补跑逐日汇总刷屏）。"""
    calls = []
    monkeypatch.setattr(runner, "_dispatch_dingtalk",
                        lambda aid, msg, ts=None: calls.append(msg))
    aux = {"replay_mode": True, "start_cash": 100000.0, "prev_close_net": None}
    runner._emit_eod_notify("aid", _eod_fake_ctx(), {"net_value": 100000.0, "pnl": 0.0},
                            aux, datetime.datetime(2026, 8, 14, 15, 5))
    assert calls == []

def test_eod_notify_dispatches_live(tmp_quant, monkeypatch):
    """实时收盘 EOD 仍推钉钉。"""
    calls = []
    monkeypatch.setattr(runner, "_dispatch_dingtalk",
                        lambda aid, msg, ts=None: calls.append(msg))
    aux = {"replay_mode": False, "start_cash": 100000.0, "prev_close_net": None}
    runner._emit_eod_notify("aid", _eod_fake_ctx(), {"net_value": 100000.0, "pnl": 0.0},
                            aux, datetime.datetime(2026, 8, 14, 15, 5))
    assert len(calls) == 1
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py::test_eod_notify_suppressed_during_replay -q`
Expected: FAIL（当前 replay_mode 分支会 dispatch）。

- [ ] **Step 3: 实现**

把 `_emit_eod_notify` 的分支改为：

```python
    aux["prev_close_net"] = net
    if aux.get("replay_mode"):
        # 补跑不发日常钉钉：长补跑逐日汇总会刷屏；异常告警（🚨【成交额异常】）已在
        # _replay_log_sink 即时推送，不经此汇总。攒批通知直接丢弃。
        _replay_day_notifies.clear()
        return
    day = str(now)[:10]
    msg = _build_daily_pnl(day, net, day_pnl, day_pct,
                           total_pnl, total_pct, holdings)
    _dispatch_dingtalk(account_id, msg, ts=str(now))
```

（删除原 `replay_mode` 分支里的 `_build_daily_summary` 调用与 `_dispatch_dingtalk`，以及 `day` 变量在原分支的位置。）

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py::test_eod_notify_suppressed_during_replay tests/quant/test_runner_strategy.py::test_eod_notify_dispatches_live -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_strategy.py
git commit -m "feat(sim): 补跑期间不发日常钉钉汇总（异常告警除外）"
```

---

### Task 2: `_replay_log_sink` 异常 notify 补跑期间即时推钉钉

**Files:**
- Modify: `backend/app/quant/simulate/runner.py:1070-1085`（`_replay_log_sink`）
- Test: `backend/tests/quant/test_runner_strategy.py`

**Interfaces:**
- Consumes: Task 1 的 `_emit_eod_notify`（不再推日常汇总）。
- Produces: 补跑期间 `log.notify` 消息以 `🚨【成交额异常】` 开头时立即 `_dispatch_dingtalk`；其它 notify 仍攒 `_replay_day_notifies`。

- [ ] **Step 1: 写失败测试**

在 `test_runner_strategy.py` 追加（复用已有 `_patch_one_loop`、`_replay_dm_cls`、`save_strategy`、`_feed_factory`、`_hist_feed` 相关脚手架）：

```python
STRATEGY_ANOMALY = '''
def init(context):
    context.universe = ["510300.XSHG"]

def morning(context):
    log.notify("🚨【成交额异常】2026-08-13 全市场ETF总成交额 1469.57亿元 (225只ETF有成交)，明显低于其他两天，疑似数据回源不完整，已剔除该日计算阈值")

run_daily(morning, "09:31")
'''


def test_replay_anomaly_notify_pushes_dingtalk(tmp_quant, monkeypatch):
    """补跑期间成交额异常 notify 必须即时推钉钉（不攒批）。"""
    calls = []
    monkeypatch.setattr(runner, "_dispatch_dingtalk",
                        lambda aid, msg, ts=None: calls.append(msg))
    today = datetime.date.today()
    days = [today - datetime.timedelta(days=n) for n in (3, 2, 1)]
    save_strategy("s_anom", "s", STRATEGY_ANOMALY)
    aid = service.account_create("acct_anom", 100000.0, 0.03, "s_anom", str(days[0]))
    _patch_one_loop(monkeypatch, pause_checks_before_loop=len(days))
    runner.run_loop(aid, dm=_replay_dm_cls(days)(), feed=_feed_factory(10.0),
                    matcher=Matcher(0.03))
    assert any("成交额异常" in m for m in calls), calls
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py::test_replay_anomaly_notify_pushes_dingtalk -q`
Expected: FAIL（当前补跑 notify 只攒批，不推）。

- [ ] **Step 3: 实现**

把 `_replay_log_sink` 的 `if level == "notify":` 分支改为：

```python
            if level == "notify":
                if msg.startswith("🚨【成交额异常】"):
                    # 异常告警例外：补跑期间也即时推钉钉（数据残缺需人工关注）
                    _dispatch_dingtalk(account_id, msg, ts=str(ts))
                else:
                    # 补跑不逐笔推钉钉：累积当日通知（汇总已不再推送，仅留档）
                    _replay_day_notifies.append((str(ts)[11:16], msg))
```

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py::test_replay_anomaly_notify_pushes_dingtalk -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_strategy.py
git commit -m "feat(sim): 补跑期间成交额异常 notify 即时推钉钉"
```

---

### Task 3: 策略 `_anomalous_etf_days` 判定辅助函数

**Files:**
- Modify: `data/quant_strategies/wufu-v5.4-ding.py`、`backend/tests/fixtures/wufu_v54/wufu-v5.4-ding.py`（两文件当前一致，同步改）
- Test: `backend/tests/quant/test_wufu_ding_strategy.py`

**Interfaces:**
- Produces: `_anomalous_etf_days(daily_totals: pd.Series, daily_counts: pd.Series) -> list[Timestamp]`——返回明显偏低的异常日（只数或金额 < 另两天较大者的 50%）。

- [ ] **Step 1: 扩展 `_FakeLog` + 写失败测试**

`test_wufu_ding_strategy.py` 的 `_FakeLog` 增加 `error`/`warning` 记录；再追加：

```python
def test_anomalous_etf_days_detects_low_count():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4.2e11, 1.5e11], index=idx)          # 08-13 金额仅 ~36%
    counts = pd.Series([1658, 1657, 225], index=idx)               # 08-13 只数仅 ~13.6%
    assert ns["_anomalous_etf_days"](totals, counts) == [idx[2]]

def test_anomalous_etf_days_no_false_positive():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4.2e11, 4.6e11], index=idx)
    counts = pd.Series([1658, 1657, 1658], index=idx)
    assert ns["_anomalous_etf_days"](totals, counts) == []

def test_anomalous_etf_days_exactly_50pct_is_ok():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4e11, 2e11], index=idx)              # 恰好 50%
    counts = pd.Series([1658, 1658, 829], index=idx)               # 恰好 50%
    assert ns["_anomalous_etf_days"](totals, counts) == []

def test_anomalous_etf_days_detects_low_money():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4.2e11, 1.9e11], index=idx)          # ~45%，低于 50%
    counts = pd.Series([1658, 1657, 1650], index=idx)              # 只数正常
    assert ns["_anomalous_etf_days"](totals, counts) == [idx[2]]
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_wufu_ding_strategy.py -q`
Expected: FAIL（`_anomalous_etf_days` 不存在）。

- [ ] **Step 3: 实现**

在两个策略文件 `calculate_global_etf_threshold` 之前加：

```python
def _anomalous_etf_days(daily_totals, daily_counts):
    """返回 3 日全市场 ETF 成交额中明显偏低的异常日（只数或金额 < 另两天较大者 50%）。

    数据回源不完整（如 08-13 仅 225 只、1469 亿，正常 ~1658 只、~4000 亿）时，只数与
    金额同时掉到正常 1/3 以下；正常日只数波动 <2%。返回 [] 表示无异常。
    """
    days = list(daily_totals.index)
    anomaly = []
    for day in days:
        others = [d for d in days if d != day]
        max_other_count = max(daily_counts.get(d, 0) for d in others)
        max_other_money = max(daily_totals[d] for d in others)
        count = daily_counts.get(day, 0)
        money = daily_totals[day]
        if (max_other_count and count < max_other_count * 0.5) \
                or (max_other_money and money < max_other_money * 0.5):
            anomaly.append(day)
    return anomaly
```

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_wufu_ding_strategy.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add data/quant_strategies/wufu-v5.4-ding.py backend/tests/fixtures/wufu_v54/wufu-v5.4-ding.py backend/tests/quant/test_wufu_ding_strategy.py
git commit -m "feat(strategy): ETF 成交额异常日判定辅助函数"
```

---

### Task 4: `calculate_global_etf_threshold` 接入异常自检

**Files:**
- Modify: `data/quant_strategies/wufu-v5.4-ding.py:416-428`、fixture 同位置
- Test: `backend/tests/quant/test_wufu_ding_strategy.py`

**Interfaces:**
- Consumes: Task 3 的 `_anomalous_etf_days`。
- Produces: `calculate_global_etf_threshold` 检测到异常天时：逐天 `log.error` + `log.notify`（文案 `🚨【成交额异常】...`），阈值改用剔除异常天后的日均值；不足 2 个正常日回落保守阈值。

- [ ] **Step 1: 写失败测试**

追加：

```python
def _threshold_ctx(prev_day=date(2026, 8, 13)):
    c = types.SimpleNamespace()
    c.previous_date = prev_day
    c.current_dt = types.SimpleNamespace()
    c.current_dt.date = lambda: prev_day
    return c


def _money_df():
    return pd.DataFrame({
        "code": ["510300.XSHG"] * 3 + ["511880.XSHG"] * 3,
        "time": pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"] * 2),
        "money": [2e11, 2.1e11, 0.75e11, 2.2e11, 2.0e11, 0.75e11],
    })


def test_threshold_excludes_anomaly_and_notifies():
    """异常天被剔除，阈值用正常两天均值；log.error 进异常标签、log.notify 推钉钉。"""
    ns = _load_strategy()
    ns["g"]._cached_etf_universe = ["510300.XSHG", "511880.XSHG"]
    ns["g"].global_threshold_divisor = 2
    ns["get_trade_days"] = lambda end_date=None, count=0: [
        date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13)]
    ns["get_data_manager"] = lambda: types.SimpleNamespace(
        get_daily_money_cached=lambda *a, **k: _money_df())
    ns["calculate_global_etf_threshold"](_threshold_ctx())
    # daily_totals：08-11=4.2e11, 08-12=4.1e11, 08-13=1.5e11（< 4.2e11*0.5=2.1e11 → 异常）
    # 剔除 08-13 后均值 = (4.2e11+4.1e11)/2 = 4.15e11；阈值 = 4.15e11 / 2
    assert ns["g"].avg_etf_money_threshold == pytest.approx(4.15e11 / 2)
    assert any("成交额异常" in m for m in ns["log"].errors)
    assert any("成交额异常" in m for m in ns["log"].notifies)
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_wufu_ding_strategy.py::test_threshold_excludes_anomaly_and_notifies -q`
Expected: FAIL（当前阈值 = 3 天均值，无 error/notify）。

- [ ] **Step 3: 实现**

把 `calculate_global_etf_threshold` 的 `len(daily_totals) < 3` 检查之后改为：

```python
        if len(daily_totals) < 3:
            log.warning(f"仅有{len(daily_totals)}个有效交易日，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        anomaly_days = _anomalous_etf_days(daily_totals, daily_counts)
        if anomaly_days:
            for day in anomaly_days:
                money = daily_totals[day]
                count = daily_counts.get(day, 0)
                msg = (f"🚨【成交额异常】{day.date()} 全市场ETF总成交额 {money/1e8:.2f}亿元 "
                       f"({count}只ETF有成交)，明显低于其他两天，疑似数据回源不完整，"
                       f"已剔除该日计算阈值")
                log.error(msg)
                log.notify(msg)
            good = [d for d in daily_totals.index if d not in anomaly_days]
            if len(good) < 2:
                log.warning("剔除异常日后不足2个正常交易日，使用保守阈值1000万")
                g.avg_etf_money_threshold = 10000000
                return
            avg_total_money = daily_totals[good].mean()
            threshold = avg_total_money / g.global_threshold_divisor
            g.avg_etf_money_threshold = threshold
            log.info(f"【全局阈值更新完成】(已剔除异常日) 近{len(good)}日全市场ETF日均总成交额="
                     f"{avg_total_money/1e8:.2f}亿元，阈值={threshold/1e4:.0f}万元({threshold:,.0f}元)")
            return
        avg_total_money = daily_totals.mean()
        threshold = avg_total_money / g.global_threshold_divisor
        g.avg_etf_money_threshold = threshold
        log.info(f"【全局阈值更新完成】近{len(daily_totals)}日全市场ETF日均总成交额={avg_total_money/1e8:.2f}亿元，阈值={threshold/1e4:.0f}万元({threshold:,.0f}元)")
```

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_wufu_ding_strategy.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add data/quant_strategies/wufu-v5.4-ding.py backend/tests/fixtures/wufu_v54/wufu-v5.4-ding.py backend/tests/quant/test_wufu_ding_strategy.py
git commit -m "feat(strategy): ETF 成交额异常自检——剔除异常日 + 异常标签 + 钉钉"
```

---

### Task 5: 全量回归 + lint

**Files:**
- 无代码改动；运行验证。

- [ ] **Step 1: 跑相关测试**

Run:
```bash
cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py tests/quant/test_wufu_ding_strategy.py tests/quant/test_runner_mark.py tests/quant/test_runner_dingtalk.py tests/quant/test_fix_datamanager.py tests/quant/test_fix_sim.py -q
```
Expected: 全 PASS

- [ ] **Step 2: 跑全量 quant（跳过 integration）**

Run: `cd backend && uv run --extra dev pytest tests/quant/ -q -m "not integration"`
Expected: 仅 3 个已知预存失败（`test_h4_h5_...`、`test_mootdx_backfill_coverage.py::test_backfill_noop_...`、`test_run_quant_backtest.py::test_valid_run_...`），其余全 PASS。

- [ ] **Step 3: lint 改动文件**

Run: `cd backend && uv run --extra dev ruff check app/quant/simulate/runner.py tests/quant/test_runner_strategy.py tests/quant/test_wufu_ding_strategy.py`
Expected: 无新增违规（RUF002/003 中文标点与仓库既有风格一致）。

- [ ] **Step 4: 提交（若步骤 1/2 无新增失败）**

```bash
git add -A
git commit -m "test: 补跑钉钉抑制 + 成交额异常自检回归"
```
