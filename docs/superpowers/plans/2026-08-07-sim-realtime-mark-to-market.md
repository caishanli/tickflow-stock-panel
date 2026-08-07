# 模拟盘实时打标 + 收盘重估 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复模拟盘今日收盘重估缺失，并在盘中提供秒级实时估值。

**Architecture:** 在 `_run_strategy_loop` 主循环内新增亚分钟 mark 步骤（每 10s 用 `live_feed.refresh` 刷新持仓价并重算净值，价格跳变超阈值才落库），新增 `_revalue_at_close` 用当日真实收盘价重打全部持仓，挂载到 `_eod` 与补跑「今天」分支末尾。

**Tech Stack:** Python 3.11, pytest, FastAPI, Polars/pandas（测试用 stub DM）。

**Spec:** `docs/superpowers/specs/2026-08-07-sim-realtime-mark-to-market-design.md`

## Global Constraints

- 测试从 `backend/` 目录运行，用 `uv run --extra dev pytest`（不要裸跑 `uv run pytest`）。
- ruff line-length 100（select E,F,I,N,UP,B,SIM,RUF，忽略 E501）；mypy 严格。
- 不改策略回调 / matcher / 撮合 / SSE 协议。前端零改动。
- 补跑期间快照必须走 `_persist` 攒批，不能直接 `db.insert_sim_snapshot`。
- 不加注释除非必要；遵循现有代码风格（函数 `_` 前缀、日志用 `log.`）。

---

### Task 1: 收盘重估 `_revalue_at_close` + `_eod` 接线

**Files:**
- Modify: `backend/app/quant/simulate/runner.py`（新增函数 + `_eod`）
- Test: `backend/tests/quant/test_runner_mark.py`（新建）

**Interfaces:**
- Consumes: `dm.get_minute_price_at(code, dt)`、`_state_from_portfolio(ctx, state)`、`_persist(...)`、`db`。
- Produces: `_revalue_at_close(dm, ctx, state, bar_dt) -> None`（重打持仓价 + 更新 state 的 net/pnl，不落库——落库由调用方 `_persist` 完成）。

- [ ] **Step 1: 写失败测试**

创建 `backend/tests/quant/test_runner_mark.py`：

```python
"""收盘重估 + 盘中实时打标测试（离线：stub DM / feed）。"""
from __future__ import annotations

import datetime

import pandas as pd
import pytest

from app.quant import db, service
from app.quant.config import CONFIG
from app.quant.simulate import protocol, runner
from app.quant.simulate.matcher import Matcher
from app.quant.strategies.store import save_strategy


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path / "strategies"))
    db.init_db(str(db_path))
    return tmp_path


class _FakeClient:
    """盘中 feed 用：current_snapshot 返回 as_of 时刻的 1 分钟价。"""

    def __init__(self, price=12.0):
        self.price = float(price)

    def current_snapshot(self, codes, as_of=None):
        idx = pd.DatetimeIndex([pd.Timestamp(as_of)])
        return {c: pd.DataFrame({"open": [self.price], "high": [self.price],
                                 "low": [self.price], "close": [self.price],
                                 "volume": [100], "amount": [100.0]},
                                index=idx) for c in codes}


class _StubDM:
    """最小 DataManager stub：收盘价由 get_minute_price_at 提供。"""

    _minute_mem = {}
    _minute_cov = {}
    _daily_mem = {}
    _daily_ver = 0

    def __init__(self, close_price=None):
        self.client = _FakeClient()
        self.close_price = close_price

    def get_minute_price_at(self, code, dt):
        if self.close_price is None:
            return None
        if pd.Timestamp(dt).time() <= datetime.time(15, 0):
            return self.close_price
        return None

    def fetch(self, method, *a, **k):
        if method == "get_daily" and a:
            # 回退源（get_minute_price_at 无收盘价时用）：单日 close
            return pd.DataFrame({"close": [self.close_price or 0.0]},
                                index=pd.DatetimeIndex([pd.Timestamp(a[-1])]))
        raise RuntimeError("stub: no data")


STRATEGY_NOOP = "def init(context):\n    context.universe = []\n"


def _st():
    return {
        "cash": 0.0, "start_cash": 50000.0, "net_value": 50000.0, "pnl": 0.0,
        "positions": {"510300.XSHG": {"amount": 5000.0, "avg_cost": 10.0,
                                      "price": 10.0, "today_amount": 0.0}},
        "stop_loss_log": [], "dt": "2026-07-17 09:31",
    }


def _revalue_at_close_setup(tmp_quant):
    save_strategy("s_rc", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_rc", 100000.0, 0.03, "s_rc")
    protocol.save_state(aid, _st())
    return aid


def test_revalue_at_close_marks_to_real_close(tmp_quant):
    """收盘重估：持仓按当日真实收盘价打标并更新 state 净值（不落库）。"""
    aid = _revalue_at_close_setup(tmp_quant)
    dm = _StubDM(close_price=12.0)
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()},
        "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0

    runner._revalue_at_close(dm, ctx, st, pd.Timestamp("2026-07-17 15:05"))

    assert st["positions"]["510300.XSHG"]["price"] == 12.0
    assert st["net_value"] == pytest.approx(5000 * 12.0)          # 0 现金 + 市值
    assert st["pnl"] == pytest.approx(5000 * 12.0 - 50000.0)
    # 重估后 pos.price 也同步（供后续 matcher/persist 用）
    assert ctx.portfolio.positions["510300.XSHG"].price == 12.0
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py::test_revalue_at_close_marks_to_real_close -q`
Expected: `AttributeError: module 'app.quant.simulate.runner' has no attribute '_revalue_at_close'`

- [ ] **Step 3: 实现 `_revalue_at_close`**

在 `runner.py` 中 `_persist` 之后新增：

```python
def _revalue_at_close(dm, ctx, state: dict, bar_dt) -> None:
    """收盘重估：把全部持仓按当日真实收盘价重打，并重算 state 净值。

    价格源：优先 ``dm.get_minute_price_at(code, 当日 15:00)``（真实 1m 收盘），
    无则回退日线当日 close（provider.get_daily 有进程内缓存）。只更新估值，
    不触发策略回调 / matcher，不落库（落库由调用方 ``_persist`` 完成）。
    """
    if dm is None:
        return
    today = pd.Timestamp(bar_dt)
    close_ts = today.replace(hour=15, minute=0, second=0, microsecond=0)
    pf = ctx.portfolio
    changed = False
    for code, pos in list(pf.positions.items()):
        price = dm.get_minute_price_at(code, close_ts)
        if price is None:
            try:
                df = dm.fetch("get_daily", code, str(today.date()), str(today.date()))
                if df is not None and not (hasattr(df, "empty") and df.empty):
                    price = float(df["close"].iloc[-1])
            except Exception as e:  # noqa: BLE001
                log.warning("[runner] %s 收盘价重估失败: %s", code, e)
        if price is None:
            log.warning("[runner] %s 收盘重估无价，保留现价 %.4f", code, pos.price)
            continue
        pos.price = float(price)
        changed = True
    if changed:
        _state_from_portfolio(ctx, state)
        start_cash = state.get("start_cash", 0.0) or 0.0
        positions_value = round(sum(p.amount * p.price for p in pf.positions.values()), 4)
        net = round(pf.cash + positions_value, 4)
        state["net_value"] = net
        state["pnl"] = round(net - start_cash, 4)
        state["dt"] = str(bar_dt)
```

注意：`_StubDM` 没有 `get_daily`，靠 `hasattr` 兜底；真实 DM 有 `get_daily(code, start, end)` 返回含 `close` 列的 DataFrame。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py::test_revalue_at_close_marks_to_real_close -q`
Expected: PASS

- [ ] **Step 5: `_eod` 接线 + 测试**

修改 `_eod`（runner.py:469），在 `_persist` 前插入 `_revalue_at_close`：

```python
def _eod(account_id: str, bundle, ctx, dm, state: dict, aux: dict, now) -> None:
    """收盘（每交易日一次）：after_close/after_trading_end + 真实分钟落盘 + 收盘重估 + 最终快照。"""
    if aux.get("replay_mode"):
        _flush_replay_batch(account_id, aux)
    for func, t in bundle.daily:
        if str(t) == "after_close":
            _safe_call(account_id, func, ctx, "after_close")
    if bundle.after_trading_end is not None:
        _safe_call(account_id, bundle.after_trading_end, ctx, "after_trading_end")
    live_feed.persist_real(dm, aux["fresh_frames"])
    _revalue_at_close(dm, ctx, state, pd.Timestamp(now))
    _persist(account_id, ctx, state, now, aux["jq_api"], aux)
    # now 在补跑时是引擎推进到的当日收市时刻（15:05），日志时间戳随引擎而非真实时钟
    _emit_log(account_id, "info", "收盘处理完成，当日真实分钟数据已落盘", ts=str(now))
```

新增测试（同一文件追加）：

```python
def test_eod_revalues_positions_to_close(tmp_quant, monkeypatch):
    """_eod 终态快照按当日收盘价打标，而非最后 feed 价。"""
    aid = _revalue_at_close_setup(tmp_quant)
    dm = _StubDM(close_price=12.0)
    bundle = type("B", (), {"daily": [], "after_trading_end": None})()
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()}, "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0
    runner._state_from_portfolio(ctx, st)
    monkeypatch.setattr(runner, "_safe_call", lambda *a, **k: None)
    runner._eod(aid, bundle, ctx, dm, st, {"fresh_frames": {}, "jq_api": object()},
                datetime.datetime(2026, 7, 17, 15, 5))

    snaps = db.get_sim_snapshots(aid)
    assert snaps[-1]["positions_value"] == pytest.approx(5000 * 12.0)
    assert snaps[-1]["net_value"] == pytest.approx(5000 * 12.0)
    assert protocol.read_state(aid)["positions"]["510300.XSHG"]["price"] == 12.0
```

- [ ] **Step 6: 运行测试**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py -q`
Expected: 2 PASS

- [ ] **Step 7: 补跑「今天」分支接线 + 测试**

修改 `_replay_history` 今天分支末尾（runner.py:688 之后）与 `_replay_partial_day`（runner.py:579 `_strategy_tick` 循环后），在 `_persist` 前补收盘重估（仅收盘后）：

`_replay_history` 今天分支内，`for bar` 循环后、`_emit_log` 前插入：

```python
            # 收盘后启动/重置账户：补跑完今天后先按真实收盘价重估，再进实时
            if now.time() > SESSION_END_GRACE:
                _revalue_at_close(dm, ctx, state,
                                  pd.Timestamp(datetime.datetime.combine(today, datetime.time(15, 5))))
                _persist(account_id, ctx, state,
                         datetime.datetime.combine(today, datetime.time(15, 5)), aux["jq_api"], aux)
```

`_replay_partial_day` 内 `for bar` 循环后、`_emit_log` 前插入同样逻辑（用同一个 `today`/`now`）。

新增测试：

```python
def test_replay_today_after_close_revalues(tmp_quant, monkeypatch):
    """收盘后补跑今天：末快照按真实收盘价，而非停在买入价。"""
    class _FixedNow(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 8, 7, 17, 0, 0)

    monkeypatch.setattr(runner.datetime, "datetime", _FixedNow)
    today = datetime.date(2026, 8, 7)
    save_strategy("s_rc2", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_rc2", 100000.0, 0.03, "s_rc2", str(today))
    protocol.save_state(aid, _st())
    dm = _StubDM(close_price=12.0)
    pauses = iter([False] * 3 + [True])  # 今天补跑逐 bar 暂停检查 + 实时段退出
    monkeypatch.setattr(runner, "is_paused", lambda aid: next(pauses))
    monkeypatch.setattr(runner, "in_trading", lambda now=None: False)
    monkeypatch.setattr(runner, "_is_trading_day", lambda dm, today: True)
    monkeypatch.setattr(runner, "_prev_close_dm", lambda dm, code, today: None)
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)

    class _ReplayDM(_StubDM):
        def __init__(self):
            super().__init__(close_price=12.0)
            # 盘中各 bar 无分钟价（模拟 517520 不在预取池 → _hist_feed 取不到价），
            # 只有 15:00 收盘价可取 —— 复现「补跑买入后 pos.price 停在买入价」的真实场景
        def get_minute_price_at(self, code, dt):
            if pd.Timestamp(dt).time() == datetime.time(15, 0):
                return self.close_price
            return None

    runner.run_loop(aid, dm=_ReplayDM(), feed=None, matcher=Matcher(0.03))

    snaps = db.get_sim_snapshots(aid)
    today_snaps = [s for s in snaps if s["dt"].startswith(str(today))]
    assert today_snaps and today_snaps[-1]["positions_value"] == pytest.approx(5000 * 12.0)
    assert protocol.read_state(aid)["positions"]["510300.XSHG"]["price"] == 12.0
```

注意：`feed=None` 时 `_run_strategy_loop` 用 `live_feed.refresh`，盘中为 False 且 today 已补跑完 → 实时段空转；`is_paused` 序列要覆盖「补跑今天循环内逐 bar 检查 + 实时段退出」的总次数。

- [ ] **Step 8: 运行测试**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py -q`
Expected: 3 PASS

- [ ] **Step 9: 回归 + Commit**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_runner_strategy.py tests/quant/test_runner_mark.py -q
cd /home/ubuntu/tickflow-stock-panel && git add backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_mark.py
git commit -m "feat(sim): 收盘重估 + 收盘后补跑按真实收盘价打标"
```

---

### Task 2: 盘中实时打标（mark 步骤）

**Files:**
- Modify: `backend/app/quant/simulate/runner.py`（常量 + `_run_strategy_loop` 主循环）
- Test: `backend/tests/quant/test_runner_mark.py`（追加）

**Interfaces:**
- Consumes: `MARK_INTERVAL`、`MARK_SNAPSHOT_TICK`、`feed`（与策略 tick 同一取价源）、`db.insert_sim_snapshot`、`save_state`。
- Produces: `_mark_to_market(feed, dm, ctx, state, last_mark, now) -> bool`（返回是否有价格跳变触发快照落库）。

- [ ] **Step 1: 写失败测试**

在 `test_runner_mark.py` 追加：

```python
def _feed(price):
    def _fe(dm, codes, now, acc):
        return {c: price for c in codes}, pd.Timestamp(now)
    return _fe


def test_mark_to_market_updates_state_and_returns_dirty(tmp_quant):
    """盘中 mark：刷新持仓价到最新、重算净值，价格跳变返回 True。"""
    aid = _revalue_at_close_setup(tmp_quant)
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()}, "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0
    runner._state_from_portfolio(ctx, st)
    last_mark = {"510300.XSHG": 10.0}

    dirty = runner._mark_to_market(_feed(12.0), None, ctx, st, last_mark,
                                   pd.Timestamp("2026-07-17 10:30"))

    assert dirty is True
    assert st["positions"]["510300.XSHG"]["price"] == 12.0
    assert st["net_value"] == pytest.approx(5000 * 12.0)
    assert last_mark["510300.XSHG"] == 12.0
    assert ctx.portfolio.positions["510300.XSHG"].price == 12.0


def test_mark_to_market_no_change_is_clean(tmp_quant):
    """价格未越过阈值时不 dirty（不落快照）。"""
    aid = _revalue_at_close_setup(tmp_quant)
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()}, "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0
    runner._state_from_portfolio(ctx, st)
    last_mark = {"510300.XSHG": 10.0}

    dirty = runner._mark_to_market(_feed(10.0), None, ctx, st, last_mark,
                                   pd.Timestamp("2026-07-17 10:30"))

    assert dirty is False
    assert st["positions"]["510300.XSHG"]["price"] == 10.0
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py -q`
Expected: `AttributeError: ... has no attribute '_mark_to_market'`

- [ ] **Step 3: 实现 mark 步骤**

在 `runner.py` 常量区新增：

```python
MARK_INTERVAL = 10          # 盘中实时打标间隔（秒）
MARK_SNAPSHOT_TICK = 0.0005 # 打标价格相对上次跳变超过该比例才落快照
```

新增函数（放在 `_strategy_tick` 之前）：

```python
def _mark_to_market(feed, dm, ctx, state: dict, last_mark: dict, now) -> bool:
    """盘中实时打标：把持仓价刷新到最新并重算净值。

    与策略 tick 共用同一 ``feed``（策略 tick 用 live_feed.refresh 实时，mark 也用
    同一 feed，保证 live 与测试/回放口径一致）。返回 True 表示任一持仓价相对上次
    打标跳变超过 MARK_SNAPSHOT_TICK（调用方据此决定是否落快照 + save_state）。
    只改估值，不触发策略/matcher。
    """
    pf = ctx.portfolio
    if not pf.positions:
        return False
    codes = list(pf.positions.keys())
    prices, _bar = feed(dm, codes, now, None)
    if not prices:
        return False
    dirty = False
    for code, pos in list(pf.positions.items()):
        px = prices.get(code)
        if px is None:
            continue
        prev = last_mark.get(code)
        if prev is None or abs(px / prev - 1) >= MARK_SNAPSHOT_TICK:
            dirty = True
        pos.price = float(px)
        last_mark[code] = float(px)
    if dirty:
        _state_from_portfolio(ctx, state)
        start_cash = state.get("start_cash", 0.0) or 0.0
        positions_value = round(sum(p.amount * p.price for p in pf.positions.values()), 4)
        net = round(pf.cash + positions_value, 4)
        state["net_value"] = net
        state["pnl"] = round(net - start_cash, 4)
        state["dt"] = str(now)
    return dirty
```

注意：`feed(dm, codes, now, None)` 第 4 参是 `fresh_acc`。`live_feed.refresh(dm, codes, now, fresh_acc=..., loader=..., enabled=...)` 的签名匹配 `(dm, codes, now, fresh_acc)`；测试的 `_feed_factory` 是 `(dm, codes, now, acc)`。保持一致。

Step 1 测试已经用 `_feed(price)`（序列化 feed）调用 `_mark_to_market(_feed(px), None, ctx, st, last_mark, now)`。并发调用方（主循环 mark 与日频分支）统一传策略 tick 用的同一个 `feed`。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py -q`
Expected: 5 PASS（Task1 3 个 + Task2 2 个）

- [ ] **Step 5: 主循环接线**

修改 `_run_strategy_loop` 主循环（runner.py:872 附近）。目标：每次策略 tick 后，用 mark 子循环填满到下一分钟边界。

替换 872-887 的区块：

```python
            if in_trading(now) or (datetime.time(15, 0) < t <= SESSION_END_GRACE):
                if aux["frequency"] == "daily" and aux["daily_done"] == today:
                    # 日频账户：当日唯一 tick 已完成，仍做实时打标
                    dirty = _mark_to_market(feed, dm, ctx, state,
                                            aux.setdefault("last_mark", {}), now)
                    if dirty:
                        save_state(account_id, state)
                        positions_value = round(sum(
                            p.amount * p.price for p in ctx.portfolio.positions.values()), 4)
                        db.insert_sim_snapshot(account_id, state["dt"], state["net_value"],
                                               state["cash"], positions_value,
                                               state["pnl"],
                                               round(state["net_value"] / state["start_cash"] - 1, 6)
                                               if state["start_cash"] else 0.0)
                    time.sleep(max(1, 60 - now.second + TICK_OFFSET))
                    continue
                bar = _strategy_tick(account_id, bundle, ctx, dm, feed, matcher,
                                     state, aux, now)
                if bar is not None and aux["frequency"] == "daily":
                    aux["daily_done"] = today
                # 对齐分钟边界 + 偏移，等刚收的 bar 可读
                sleep_left = max(1, 60 - now.second + TICK_OFFSET)
                while sleep_left > 0:
                    step = min(MARK_INTERVAL, sleep_left)
                    time.sleep(step)
                    sleep_left -= step
                    if not in_trading() or not ctx.portfolio.positions:
                        continue
                    mnow = datetime.datetime.now()
                    dirty = _mark_to_market(feed, dm, ctx, state,
                                            aux.setdefault("last_mark", {}), mnow)
                    if dirty:
                        save_state(account_id, state)
                        positions_value = round(sum(
                            p.amount * p.price for p in ctx.portfolio.positions.values()), 4)
                        db.insert_sim_snapshot(account_id, state["dt"], state["net_value"],
                                               state["cash"], positions_value,
                                               state["pnl"],
                                               round(state["net_value"] / state["start_cash"] - 1, 6)
                                               if state["start_cash"] else 0.0)
```

说明：
- 第一次 mark 后 `last_mark` 记录当日初始价，随后价格未跳变不落快照；60s tick 到达时
  `_strategy_tick` 照常驱动策略/matcher（bar 去重保证不重复触发）。
- mark 写 `save_state` 触发 SSE `status` 事件（`sim_stream` 按 state 签名变化推送），前端卡片实时刷新；
  快照按 `MARK_SNAPSHOT_TICK` 阈值节流。
- `time.sleep` 仍用 `runner.time.sleep`（测试已 patch）。
- 日频账户 tick 后 continue 分支同样补一次 mark，保证日频账户也能实时估值。

- [ ] **Step 6: 主循环接线回归测试**

在 `test_runner_mark.py` 追加：

```python
def test_strategy_loop_live_marks_positions(tmp_quant, monkeypatch):
    """盘中实时：两次 tick 之间持仓价随最新行情打标，净值跟涨。"""
    save_strategy("s_mk", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_mk", 100000.0, 0.03, "s_mk")
    protocol.save_state(aid, _st())
    from app.quant.jqengine.engine.jq.context import Position
    pauses = iter([False] * 20 + [True])
    monkeypatch.setattr(runner, "is_paused", lambda aid: next(pauses))
    monkeypatch.setattr(runner, "in_trading", lambda now=None: True)
    monkeypatch.setattr(runner, "_is_trading_day", lambda dm, today: True)
    monkeypatch.setattr(runner, "_prev_close_dm", lambda dm, code, today: None)
    sleep_log = []
    monkeypatch.setattr(runner.time, "sleep", lambda s: sleep_log.append(s))

    class _FixedNow(datetime.datetime):
        current = datetime.datetime(2026, 7, 17, 10, 30, 0)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(runner.datetime, "datetime", _FixedNow)

    class _LiveDM(_StubDM):
        def __init__(self):
            super().__init__()
            self.client = _FakeClient(price=12.0)

    runner.run_loop(aid, dm=_LiveDM(), feed=None, matcher=Matcher(0.03))

    st = protocol.read_state(aid)
    assert st["positions"]["510300.XSHG"]["price"] == 12.0
    assert st["net_value"] == pytest.approx(5000 * 12.0)
    snaps = db.get_sim_snapshots(aid)
    assert snaps and snaps[-1]["positions_value"] == pytest.approx(5000 * 12.0)
```

注意：此测试中 `_strategy_tick` 因 `feed` 无数据返回 None（`_StubDM` 无 `current_snapshot` 所需数据？——`live_feed.refresh` 走 `_FakeClient.current_snapshot`，有数据），需确认 bar 时刻推进语义：`_FixedNow.now` 固定，`_strategy_tick` 内 `last_bar` 去重 → 首轮 feed 后后续 tick 被去重，mark 子循环每轮仍跑。若 flaky，改为直接断言「state 与快照价 = 12.0、且快照数 > 0」。

- [ ] **Step 7: 运行全部测试**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py tests/quant/test_runner_strategy.py -q`
Expected: 全 PASS

- [ ] **Step 8: lint + typecheck**

```bash
cd backend && uv run --extra dev ruff check app/quant/simulate/runner.py
cd backend && uv run --extra dev mypy app/quant/simulate/runner.py
```

- [ ] **Step 9: Commit**

```bash
cd /home/ubuntu/tickflow-stock-panel && git add backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_mark.py
git commit -m "feat(sim): 盘中实时打标，价格跳变按阈值节流落快照"
```
