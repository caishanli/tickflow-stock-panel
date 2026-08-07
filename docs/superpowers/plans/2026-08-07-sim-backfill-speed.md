# 模拟盘补跑提速 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 wufu-v5.2 模拟盘从 07-10 补跑到当前的账户 start/reset 墙钟耗时降到 < 2 分钟（现在远超），且交易/净值/快照/日志逐笔与现状可复现。

**Architecture:** 复用 DM 已存在的 `set_minute_window`/`preload_minute_for_pool`——回测已靠它在启动时钉住整段区间、一次性批量预取分钟线进 `_minute_mem`。模拟盘补跑 `_replay_history` 现在从不钉窗口，走 15 天滑动窗口，导致每补跑一个交易日 DAILY 前移 → `_minute_cached` 覆盖 miss → 池内全部 ~116 只标的重复网络回源。改法是在 `_replay_history/_replay_partial_day` 补跑前钉整个区间窗口并批量预取池，之后逐 bar `get_minute_price_at`/策略自带 `preload_minute_for_pool` 全部内存命中。不改 backtest 路径、不改取数口径。

**Tech Stack:** Python 3 / pandas / pytest(backend, uv)。文件：`backend/app/quant/simulate/runner.py`、`backend/tests/quant/test_runner_strategy.py`。

## Global Constraints

- 测试从 `backend/` 运行：`uv run --extra dev pytest tests/quant/test_runner_strategy.py`，`asyncio_mode="auto"`。
- 只改模拟盘补跑路径；**不改 backtest 路径**（`rqalpha_bridge`/`set_minute_window` 现有调用行为不动）。
- 不改取数口径（真实 mootdx 分钟、前复权、拆分调整逻辑不变），成交/净值逐笔必须与现状一致。
- 通过 `_StubDM` 上没有 `set_minute_window/preload_minute_for_pool`，实现必须用 `getattr(dm, ..., None)` 兜底，保证既有测试（stub 无此方法）不崩。
- lint：`uv run --extra dev ruff check app`（line-length 100）。现有风格 `from __future__ import annotations`。

---
## Task 1: 补跑前钉分钟窗口 + 批量预取池

**Files:**
- Modify: `backend/app/quant/simulate/runner.py`（`_replay_history` 与 `_replay_partial_day` 头部）
- Test: `backend/tests/quant/test_runner_strategy.py`

**Interfaces:**
- Consumes: `dm.set_minute_window(start, end)`、`dm.preload_minute_for_pool(codes, as_of)`（`manager.py` 已有，不再实现）。
- Produces: `runner._pin_replay_minute_window(dm, ctx, start, end) -> None`——在每个补跑入口调用，`hasattr` 兜底对 stub 无害。`_replay_history(ctx, dm, ...)` 与 `_replay_partial_day(ctx, dm, ...)` 都已持有 `ctx` 形参，直接传入，不再从 `aux` 读。

- [ ] **Step 1: 写失败测试（`_replay_dm_cls` 增加记录窗口的桩 + 新用例）**

在 `_replay_dm_cls` 里给 stub DM 加 `set_minute_window` / `preload_minute_for_pool` 记录桩，并新增一个用例断言补跑把整个区间钉为窗口。`_replay_dm_cls` 修改处（`test_runner_strategy.py:277-294`）改为：

```python
def _replay_dm_cls(days):
    """带历史日线（交易日判定/取交易日表）与历史分钟价的 stub DM。

    附 set_minute_window / preload_minute_for_pool 桩：记录补跑是否把整个区间钉窗
    口并批量预取池（Task 1 提速的核心行为）。
    """

    class _ReplayDM(_StubDM):
        DAYS = days

        def __init__(self):
            super().__init__()
            self.window_seen = None
            self.pool_seen = None

        def set_minute_window(self, start, end):
            self.window_seen = (str(start)[:10], str(end)[:10])

        def preload_minute_for_pool(self, codes, as_of=None):
            self.pool_seen = list(codes) if codes else []

        def fetch(self, method, *a, **k):
            if method == "get_daily" and a and a[0] == "000300.XSHG":
                return pd.DataFrame({"close": [1.0] * len(self.DAYS)},
                                    index=pd.DatetimeIndex([str(d) for d in self.DAYS]))
            raise RuntimeError("stub: no data")

        def get_minute_price_at(self, code, dt):
            if pd.Timestamp(dt).date() in self.DAYS:
                return 10.0
            return None

    return _ReplayDM
```

新增用例（加在 `test_strategy_loop_replays_history_then_live` 之后）：

```python
def test_replay_pins_minute_window_and_preloads_pool(tmp_quant, monkeypatch):
    """补跑前必须钉住整个区间分钟窗口并批量预取池，否则滑窗导致池内标的逐日网络回源。"""
    today = datetime.date.today()
    days = [today - datetime.timedelta(days=n) for n in (4, 3, 2, 1)]
    save_strategy("s_pin", "s", STRATEGY_BUY)
    aid = service.account_create("acct_pin", 100000.0, 0.03, "s_pin", str(days[0]))
    _patch_one_loop(monkeypatch, pause_checks_before_loop=len(days))
    dm = _replay_dm_cls(days)()
    runner.run_loop(aid, dm=dm, feed=_feed_factory(10.0), matcher=Matcher(0.03))

    assert dm.window_seen is not None
    assert dm.window_seen[0] == str(days[0])
    # 窗口上界 = 今天（start_date 在今天内，整个补跑区间被钉住）
    assert dm.window_seen[1] == str(today)
    assert dm.pool_seen is not None
    assert "510300.XSHG" in dm.pool_seen           # 策略 universe 标的被批量预取
```

说明：`account_create` 在文件内已 `from app.quant import service`；直接用 `service.account_create`（见既有用例风格，避免重名遮蔽）。

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py::test_replay_pins_minute_window_and_warmups_pool -q`
Expected: FAIL（`dm.window_seen is None`——补跑从不调 `set_minute_window`）。

- [ ] **Step 3: 实现 `_pin_replay_window` 并接入 `_replay_history` / `_replay_partial_day`**

在 `runner.py` 的 `_replay_history` 函数上方（补跑分区函数定义之间）加一个辅助函数：

```python
def _pin_replay_minute_window(dm, ctx, start, end) -> None:
    """补跑前把整个补跑区间钉为分钟窗口并批量预取池，消除逐日滑动窗口的重复回源。

    回测由 rqalpha_bridge 在启动前 set_minute_window；模拟盘补跑此处补上同口径。
    用 hasattr 兜底：stub DM（既有测试）无此方法时静默跳过，不影响行为。
    """
    if dm is None:
        return
    set_win = getattr(dm, "set_minute_window", None)
    if set_win is not None:
        try:
            set_win(str(start)[:10], str(end)[:10])
        except Exception:  # noqa: BLE001
            pass
    codes = []
    if ctx is not None:
        pf = getattr(ctx, "portfolio", None)
        codes = list(dict.fromkeys(
            list(getattr(ctx, "universe", None) or [])
            + (list(pf.positions.keys()) if pf is not None else [])))
    preload = getattr(dm, "preload_minute_for_pool", None)
    if preload is not None and codes:
        try:
            preload(codes, pd.Timestamp(end))
        except Exception:  # noqa: BLE001
            pass
```

然后在 `_replay_history`（`_emit_log("开始历史补跑"...)` 之前）调用，把 `today`（引擎目标日）当窗口上界；在 `_replay_partial_day`（进入 try 设 `replay_mode` 后）同样调用，`end` 用当天。两处 `start`/`end` 均为引擎推进的目标交易日（`date`/`str` 皆可，内部 `[:10]` 归一）。

- [ ] **Step 4: 运行既有全量 runner 测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py -q`
Expected: PASS（新增用例 + 旧用例全部绿）。

- [ ] **Step 5: lint + mypy**

Run: `cd backend && uv run --extra dev ruff check app/quant/simulate/runner.py && uv run --extra dev mypy app/quant/simulate/runner.py`
Expected: 0 错误（首行若报 F-shape 的未用 import 等按提示清掉）。

- [ ] **Step 6: 提交**

```bash
git add backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_strategy.py
git commit -m "fix(sim): 补跑前钉住分钟窗口并批量预取池，消除逐日重复回源"
```

## Task 2: 端到端验收 + 对齐验证

**Files:**
- 不改代码；运行验收命令。

**Interfaces:**
- Consumes: Task 1 的窗口钉定行为。
- Produces: 验证结果（无误）。

- [ ] **Step 1: 回归既有对齐门禁**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_wufu_backtest_perf.py -m integration -q`
Expected: PASS（回测性能门禁不受影响——backtest 路径未动）。

- [ ] **Step 2: 模拟盘对齐门禁（17 笔）**

按 AGENTS 小节「验收命令」，用 `run_quant_sim.py --account wufu_v5.2_sim --strategy tests/fixtures/wufu_v52/wufu-v5.2.py --date 2026-07-10`（若账户已存在，先 reset）跑一遍，`diff_jq_vs_local`/对齐脚本比对 fixtures下 `sim_260710/live_transaction_list.csv`：
Expected: 交易组逐笔对齐（17 笔），成交与净值与现状一致。补跑墙钟明显缩短（复盘日志中 `start→done` 间隔）。

- [ ] **Step 3: 实机 07-10 → today 计时**

在已建 wufu v5.2 模拟盘上 start/reset，测量 `start` 到 `历史补跑完成` 日志墙钟。
Expected: < 2 分钟（含今日回补 + 实时握手），且交易/净值与补跑前一致。

- [ ] **Step 4: （如果有新改动）提交**

若验收中发现需微调 runner，把修复随 Task 1 提交 amend 或新 commit（尽量不改口径）。

## 不做（YAGNI）

- 不改 backtest 路径，不引入 asyncio/并行预取，不加本地分钟缓存文件。
- 不引入超大宗标的的分页（现状 ~116 只内存可控；若个别策略池超大再单独评审）。

## Self-Review

- **Spec coverage**: 方案①（钉窗口）→ Task1；②（批量预取池）→ Task1；正确性保证（searchsorted 不漏未来/窗口外自愈）→ 不编码，属机制既有，验收 Task2 对齐确认；内存风险 → Task2 Step3 实机确认；不做清单 → 符合。
- **Placeholder**: 无 TBD/TODO；每步各实际断言与命令。
- **Type consistency**: `_pin_replay_minute_window(dm, start, end, aux)`——注意实际实现里我用的参数名是 `(dm, start, end, aux)` 顺序与 Step1/Step3 一致；`window_seen[0]/[1]` 与 `_pin` 的 `[:10]` 截断口径一致（`end` 在补跑 = 引擎目标日，含今天，`window_seen[1]==str(today)`）。测试断言与实现保持同一口径。