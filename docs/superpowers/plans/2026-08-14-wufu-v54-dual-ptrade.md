# wufu v5.4 双持仓 PTrade 可运行版 + 引擎适配 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 产出可直接粘贴到真实 PTrade 的 wufu v5.4 双持仓策略文件，并让本地引擎（回测 rqalpha + 模拟盘本地引擎）直接跑该文件，收益/成交与 JQ 双持仓策略对齐。

**Architecture:** 双轨镜像 JQ——回测走 rqalpha + 新 `app/quant/ptradecompat.py`（PTrade API 兼容层，策略侧 PTrade 代码、数据层转 rqalpha 原生 ID）；模拟盘走新 `app/quant/ptradeengine/`（镜像 `jqengine/engine/jq/`）+ `simulate/runner.py` 加 flavor 路由与代码转换钩子。策略文件全程 `.SS/.SZ` 代码域，仅在数据层边界翻译。

**Tech Stack:** Python（uv，`--extra dev`）/ rqalpha 6.2.1 / pandas / pytest / 共享 DataManager。

## Global Constraints

- 后端命令一律 `cd backend && uv run --extra dev ...`（dev 依赖 pytest/ruff/mypy 不在基础 venv）。
- ruff：line-length 100，select E,F,I,N,UP,B,SIM,RUF，忽略 E501。新代码 `uv run --extra dev ruff check app tests`。
- mypy：`uv run --extra dev mypy app`（新文件必须过）。
- **ptrade 策略文件规则**：`%` 格式化日志（禁 f-string）；无 `jqdata`/`record`/`set_option`/`set_order_cost`/`get_price`/`get_current_data`/`attribute_history` 等聚宽独有调用；代码 `.SS/.SZ`（`_pt()` 转换）；停牌用 `get_stock_status`、涨跌停用日线 `high_limit/low_limit`。
- 引擎内部（本地 ptradeengine）与 runner 数据触点保持 JQ 域（`.XSHG/.XSHE`），strategy 面向 API 用 PTrade 域，`conv` 钩子在边界翻译。
- 每次 task 结束必须跑相关测试并通过后 commit。
- 参照物文件（只读，勿改）：`jqcompat.py`、`jqengine/engine/jq/{api,loader,context,portfolio}.py`、`wufu-v5.4.ptrade.py`、`wufu-v5.4-dual-adapt.py`。

---
## File Structure

**Phase 1（回测路径）：**
- Create `backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py` — ptrade 双持仓策略（自包含）
- Create `backend/app/quant/ptradecompat.py` — rqalpha PTrade 兼容层（镜像 jqcompat）
- Modify `backend/app/quant/rqalpha_bridge.py` — `run_ptrade_backtest`（镜像 `run_jq_backtest`/`_run_jq_backtest_inner`）
- Create `backend/scripts/run_ptrade_rqalpha.py` — 镜像 `run_jq_rqalpha.py`
- Create `backend/tests/quant/test_ptrade_strategy_file.py`、`test_ptradecompat.py`

**Phase 2（模拟盘路径）：**
- Create `backend/app/quant/ptradeengine/__init__.py`、`context.py`、`ptrade_api.py`、`ptrade_loader.py`
- Modify `backend/app/quant/simulate/runner.py` — flavor 路由 + conv 钩子
- Create `backend/tests/quant/test_ptradeengine.py`

---
### Task 1: ptrade 双持仓策略文件

**Files:**
- Create: `backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py`
- Test: `backend/tests/quant/test_ptrade_strategy_file.py`

**Interfaces:**
- Consumes: `wufu-v5.4.ptrade.py`（单持仓 ptrade 底座）、`wufu-v5.4-dual-adapt.py`（JQ 双持仓增量）
- Produces: `initialize(context)` / `before_trading_start(context, data)` / `after_trading_end(context)` / `handle_data(context, data)` / `select_cross_asset_dual(...)`；`g.holdings_num=2`、`g.target_weights`、`cross_*` 参数

- [ ] **Step 1: Write the failing test**

`backend/tests/quant/test_ptrade_strategy_file.py`：
```python
"""ptrade 双持仓策略文件验证：编译 + API 清单（无聚宽独有调用）+ 双持仓配置。"""
import py_compile
from pathlib import Path

STRATEGY = Path(__file__).parent.parent / "fixtures" / "dual_v54" / "wufu-v5.4-dual-adapt.ptrade.py"
_JQ_ONLY = ["jqdata", "get_current_data", "attribute_history", "get_price(", "record(",
            "set_option", "set_order_cost", "get_all_securities(", "get_security_info",
            "run_daily(morning_routine", "every_bar"]


def test_compiles():
    assert STRATEGY.exists()
    py_compile.compile(str(STRATEGY), doraise=True)


def test_no_jq_apis():
    src = STRATEGY.read_text(encoding="utf-8")
    for kw in _JQ_ONLY:
        assert kw not in src, kw


def test_dual_position_config():
    src = STRATEGY.read_text(encoding="utf-8")
    assert "g.holdings_num = 2" in src
    assert "cross_slot1_floor" in src
    assert "cross_adaptive" in src
    assert "g.target_weights = [0.5, 0.5]" in src
    assert "select_cross_asset_dual" in src
    assert "run_daily(context, afternoon_routine, time='13:10')" in src
    assert "run_daily(context, sell_routine, time='13:10')" in src
    assert "run_daily(context, buy_routine, time='13:10')" in src


def test_no_fstring_log():
    """PTrade 日志用 % 格式化，不引入 f-string。"""
    src = STRATEGY.read_text(encoding="utf-8")
    for line in src.splitlines():
        if "log." in line and "f\"" in line or "log." in line and "f'" in line:
            raise AssertionError("策略日志不应使用 f-string: %s" % line)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptrade_strategy_file.py -q`
Expected: FAIL（文件不存在）

- [ ] **Step 3: Create the strategy file**

复制 `backend/tests/fixtures/wufu_v54/wufu-v5.4.ptrade.py` 为 `backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py`，然后应用以下改动：

**(a) 文件头注释**：标题改「【五福闹新春】v5.4（双持仓自适应版）— PTrade 移植版」，标注「自 JQ 版 `wufu-v5.4-dual-adapt.py` 移植双持仓逻辑」。

**(b) initialize 参数**：
- `g.holdings_num = 1` → `g.holdings_num = 2`
- 在 `g.holdings_num = 2` 下方新增：
```python
    g.cross_slot1_floor = 0.3          # slot1 另一资产大类动量下限
    g.cross_slot1_retain_ratio = 0.85  # slot1 保留粘性：现有同类持仓 ≥ 类首×该值保留
    g.cross_adaptive = True            # 自适应权重：强腿多得
    g.cross_weight_cap = 0.85          # slot0 权重上限
    g.target_weights = [0.5, 0.5]      # 默认双持仓等权（select_cross_asset_dual 会覆盖）
```
- 初始化日志 `- 持仓数量: %d只` 行下方补：
```python
- 双持仓: 跨资产第二腿(slot1)下限: %.2f 保留粘性: %.2f 自适应: %s 权重上限: %.2f
```

**(c) 调度**（替换 initialize 里三行 run_daily）：
```python
    run_daily(context, check_weak_period_daily, time='09:40')
    run_daily(context, afternoon_routine, time='13:10')
    run_daily(context, sell_routine, time='13:10')
    run_daily(context, buy_routine, time='13:10')
```

**(d) get_final_ranked_etfs 第 4 步替换**：把当前「第四步：结合当前持仓进行调整」整块（从 `# ========== 第四步：结合当前持仓进行调整 ==========` 到 `return final_result` 前的 `log_buffer.append("【最终目标】...` 循环之前，即 candidate_dict/retained/A2 持仓宽容 全部逻辑）替换为：
```python
    # ========== 第四步：跨资产双持仓选择 ==========
    log_buffer.append("")
    log_buffer.append(">>> 第四步：跨资产双持仓选择 <<<")
    current_holdings = list(_positions_map().keys())
    log_buffer.append("当前持仓ETF：%s" % current_holdings)
    final_result = select_cross_asset_dual(
        current_holdings, filtered_list, score_key, log_buffer)
    log_buffer.append("==================================================")
    full_log = "\n".join(log_buffer)
    log.info(full_log)
    return final_result
```
注意：删除旧第 4 步后 `candidate_pool` 变量不再被使用，但第 3 步仍生成它（日志展示），保留第 3 步原样。

**(e) execute_buy_trades 改为槽位加权分配**：把 for 循环内目标市值计算段替换为：
```python
        remaining_to_buy = num_etfs_to_buy - i
        # 槽位加权分配：新买入槽位目标市值 = 总资产 × 槽位权重(target_weights);
        # 单持仓退化 weights=[1.0] -> 全仓。最后一笔用剩余现金消化余量。
        slot = actual_holding_count + i
        total_value = _get_total_value(context)
        _weights = getattr(g, 'target_weights', None)
        if i == num_etfs_to_buy - 1:
            target_value_for_this_etf = remaining_cash
        elif _weights and len(_weights) > 1:
            _w = _weights[slot] if slot < len(_weights) else 1.0 / g.holdings_num
            target_value_for_this_etf = min(remaining_cash, total_value * _w)
        else:
            target_value_for_this_etf = remaining_cash // remaining_to_buy

        # 最后一笔可使用剩余全部现金，但确保不小于最小交易额
        if target_value_for_this_etf < g.min_money and remaining_cash >= g.min_money:
            target_value_for_this_etf = remaining_cash
```

**(f) 新增 select_cross_asset_dual（放在 get_final_ranked_etfs 之后）**：
```python
def select_cross_asset_dual(current_holdings, filtered_list, score_key, log_buffer):
    """跨资产双持仓选择(自适应权重):
    - slot0 = 全池动量第一;现有 top10 持仓且得分≥第一×0.9 时保留
    - slot1 = 另一资产大类动量第一,需动量≥floor;现有同类持仓≥类首×0.85 保留
    - 权重按动量比自适应: 强腿多得(弱腿仅小仓),避免半仓一个 bet 摊薄收益
    """
    filtered_sorted = sorted(filtered_list,
                             key=lambda x: x.get(score_key, float('-inf')), reverse=True)
    if not filtered_sorted:
        log_buffer.append("【双持仓选择】无过滤后候选，空仓")
        g.target_weights = [1.0]
        return []
    if getattr(g, 'holdings_num', 1) == 1:
        g.target_weights = [1.0]
        return filtered_sorted[:1]

    global_set = set(getattr(g, 'global_etf_pool', []))
    slot1_floor = getattr(g, 'cross_slot1_floor', 0.0)
    slot1_retain = getattr(g, 'cross_slot1_retain_ratio', 0.85)
    adapt = getattr(g, 'cross_adaptive', True)
    log_buffer.append("【双持仓选择】slot1下限=%.2f 自适应=%s" % (slot1_floor, adapt))

    def _is_global(code):
        return code in global_set

    top = filtered_sorted[0]
    slot0 = top
    top10_dict = {m['etf']: m for m in filtered_sorted[:10]}
    held_scored = []
    for h in current_holdings:
        m = top10_dict.get(h)
        if m is not None:
            held_scored.append(m)
    if held_scored:
        best_held = max(held_scored, key=lambda x: x.get(score_key, float('-inf')))
        if best_held.get(score_key, float('-inf')) >= top.get(score_key, float('-inf')) * 0.9:
            slot0 = best_held
            log_buffer.append("【保留 slot0】%s(%s) 得分%.4f ≥ 第一×0.9" % (
                best_held['etf_name'], best_held['etf'], best_held.get(score_key, 0)))

    other_class = [m for m in filtered_sorted
                   if _is_global(m['etf']) != _is_global(slot0['etf'])
                   and m.get(score_key, float('-inf')) >= slot1_floor]
    slot1 = None
    if other_class:
        other_top = other_class[0]
        other_top_score = other_top.get(score_key, float('-inf'))
        slot1 = other_top
        for m in other_class:
            if m['etf'] in current_holdings and m.get(score_key, float('-inf')) >= other_top_score * slot1_retain:
                slot1 = m
                log_buffer.append("【保留 slot1】%s(%s) 得分%.4f" % (
                    m['etf_name'], m['etf'], m.get(score_key, 0)))
                break
    if slot1 is None:
        g.target_weights = [1.0]
        log_buffer.append("【双持仓选择】slot1 空缺 → 退化为单持仓: %s(%s)" % (
            slot0['etf_name'], slot0['etf']))
        return [slot0]
    if slot1.get(score_key, float('-inf')) > slot0.get(score_key, float('-inf')):
        slot0, slot1 = slot1, slot0
        log_buffer.append("【双持仓选择】slot1 动量反超，交换 slot0/slot1")
    if adapt:
        s0 = max(float(slot0.get(score_key, 0.0)), 0.01)
        s1 = max(float(slot1.get(score_key, 0.0)), 0.01)
        w1 = s0 / (s0 + s1)
        w1 = max(0.5, min(getattr(g, 'cross_weight_cap', 0.85), w1))
        w2 = round(1.0 - w1, 3)
        w1 = round(w1, 3)
    else:
        w1, w2 = 0.5, 0.5
    g.target_weights = [w1, w2]
    log_buffer.append("【双持仓选择】权重 %.3f/%.3f" % (w1, w2))
    log_buffer.append("【最终目标】共2只ETF：")
    for i, item in enumerate([slot0, slot1]):
        cls = '全球/海外' if _is_global(item['etf']) else '大A/港股'
        log_buffer.append("  %d. %s(%s) [%s] %s: %.4f" % (
            i + 1, item['etf_name'], item['etf'], cls, score_key, item.get(score_key, 0)))
    return [slot0, slot1]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptrade_strategy_file.py -q`
Expected: PASS（全部 4 个用例）

- [ ] **Step 5: Commit**

```bash
git add backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py backend/tests/quant/test_ptrade_strategy_file.py
git commit -m "feat(strategy): wufu v5.4 双持仓 ptrade 移植版（跨资产双持仓 + 槽位加权分配）"
```

---
### Task 2: ptradecompat 骨架 + 代码转换 + get_history + 调度

**Files:**
- Create: `backend/app/quant/ptradecompat.py`
- Test: `backend/tests/quant/test_ptradecompat.py`

**Interfaces:**
- Consumes: rqalpha 6.2.1 API（`register_api`、`Environment`、`EVENT`、`history_bars_batch`）；数据源在 bridge 层注入
- Produces:
  - `install_ptradecompat(universe, names=None, benchmark="510300.SS", list_dates=None)`
  - `_to_jq(code) -> str` / `_to_pt(code) -> str`（`.SS<->.XSHG`、`.SZ<->.XSHE`）
  - `get_history(count, frequency, field, security_list=None, include=True, fq='pre') -> DataFrame(index=datetime, columns=PTrade码)`
  - `run_daily(context, func, time='HH:MM')` → 注册 `_DAILY_AT[(h,m)].append(func)`
  - `_install_barcache_mod()`（注册 `rqalpha_mod_ptradebarcache`，监听 BAR/BEFORE_TRADING/AFTER_TRADING 触发 `_DAILY_AT`）
  - `_ptrade_adapt_bar_dict(bar_dict) -> {PTrade码: SimpleNamespace(dt,open,high,low,close,price,volume,money,name)}`
  - `_patch_rqalpha_objects()`（StrategyContext.blotter、Portfolio.portfolio_value 等）

- [ ] **Step 1: Write the failing test**

`backend/tests/quant/test_ptradecompat.py`（代码转换 + get_history 形状，纯逻辑不依赖 rqalpha）：
```python
"""ptradecompat 纯逻辑单测：代码转换、get_history 宽表组装、调度注册、bar_dict 适配。"""
import sys
import types
from datetime import datetime

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def _fake_rqalpha(monkeypatch):
    """最小 rqalpha 桩：让 import 与 register_api 可用。"""
    rq = types.ModuleType("rqalpha")
    rq_ver = sys.modules.get("rqalpha")
    if rq_ver is None:
        rq.api = types.ModuleType("rqalpha.api")
        rq.api.register_api = lambda *a, **k: None
        rq.core = types.ModuleType("rqalpha.core")
        rq.core.events = types.ModuleType("rqalpha.core.events")
        rq.core.events.EVENT = types.SimpleNamespace(BAR="bar", BEFORE_TRADING="bt", AFTER_TRADING="at")
        rq.const = types.ModuleType("rqalpha.const")
        rq.const.INSTRUMENT_TYPE = rq.const.MARKET = rq.const.TRADING_CALENDAR_TYPE = types.SimpleNamespace()
        rq.environment = types.ModuleType("rqalpha.environment")
        rq.environment.Environment = type("Env", (), {"get_instance": staticmethod(lambda: None)})
        rq.interface = types.ModuleType("rqalpha.interface")
        rq.interface.AbstractMod = type("AbstractMod", (), {})
        rq.model = types.ModuleType("rqalpha.model")
        rq.model.instrument = types.ModuleType("rqalpha.model.instrument")
        rq.model.instrument.Instrument = object
        monkeypatch.setitem(sys.modules, "rqalpha", rq)
    import app.quant.ptradecompat as pc
    return pc


def test_code_conversion(_fake_rqalpha):
    assert _fake_rqalpha._to_jq("510300.SS") == "510300.XSHG"
    assert _fake_rqalpha._to_jq("159915.SZ") == "159915.XSHE"
    assert _fake_rqalpha._to_pt("510300.XSHG") == "510300.SS"
    assert _fake_rqalpha._to_pt("159915.XSHE") == "159915.SZ"
    assert _fake_rqalpha._to_jq("510300.XSHG") == "510300.XSHG"  # 幂等


def test_get_history_wide(_fake_rqalpha, monkeypatch):
    bars = {
        "510300.XSHG": np.array(
            [(20260101100000, 3.0, 3.1), (20260102100000, 3.2, 3.3)],
            dtype=[("datetime", "int64"), ("close", "f8"), ("volume", "f8")]),
        "159915.XSHE": np.array(
            [(20260101100000, 2.0, 5.0), (20260102100000, 2.1, 5.5)],
            dtype=[("datetime", "int64"), ("close", "f8"), ("volume", "f8")]),
    }
    class _DS:
        def history_bars_batch(self, codes, count, freq, fields, end_dt):
            return bars
    class _ENV:
        trading_dt = pd.Timestamp("2026-01-02 10:00")
    monkeypatch.setattr(_fake_rqalpha, "_history_bars_batch",
                        lambda codes, count, freq, fields, end_dt: _DS().history_bars_batch(codes, count, freq, fields, end_dt))
    df = _fake_rqalpha._build_history_wide(bars, ["510300.XSHG", "159915.XSHE"], "close")
    assert list(df.columns) == ["510300.SS", "159915.SZ"]
    assert df.index[0] == pd.Timestamp("2026-01-01 10:00:00")
    assert float(df["510300.SS"].iloc[-1]) == 3.2


def test_run_daily_registers_order(_fake_rqalpha):
    _fake_rqalpha._DAILY_AT.clear()
    calls = []
    def cb(context):
        calls.append(1)
    _fake_rqalpha.run_daily(None, cb, time="13:10")
    assert _fake_rqalpha._DAILY_AT[(13, 10)] == [cb]


def test_adapt_bar_dict(_fake_rqalpha):
    from types import SimpleNamespace
    bd = {"510300.XSHG": SimpleNamespace(open=1.0, high=1.2, low=0.9, close=1.1,
                                          volume=100, total_turnover=1.1e5)}
    out = _fake_rqalpha._ptrade_adapt_bar_dict(bd)
    assert "510300.SS" in out
    assert out["510300.SS"].money == pytest.approx(1.1e5)
    assert out["510300.SS"].price == pytest.approx(1.1)
```
（注：为让测试不依赖真实 rqalpha，`ptradecompat` 内部把「取数」抽成 `_history_bars_batch` 模块函数，测试 monkeypatch 它；`_build_history_wide` 纯函数。）

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradecompat.py -q`
Expected: FAIL（module not found）

- [ ] **Step 3: Implement ptradecompat 骨架**

参照 `jqcompat.py` 结构。`backend/app/quant/ptradecompat.py` 核心内容：
```python
"""PTrade → rqalpha 6.2.1 兼容层（镜像 jqcompat）。

让 PTrade 风格策略（def initialize / before_trading_start / after_trading_end /
handle_data + run_daily(context, func, time) + get_history/order/get_positions）
直接在 rqalpha 上回测。strategy 侧全部 PTrade 代码（.SS/.SZ），内部与 rqalpha
（order_book_id .XSHG/.XSHE）交界处转换。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import sys
import types

import numpy as np
import pandas as pd

logger = logging.getLogger("ptradecompat")

_DAILY_AT = {}   # (hour, minute) -> [func]（同一时刻按注册顺序触发）
_EVERY_BAR_CALLBACKS = []
_BENCHMARK = "510300.SS"


def _to_jq(code):
    return str(code).replace(".SS", ".XSHG").replace(".SZ", ".XSHE")


def _to_pt(code):
    return str(code).replace(".XSHG", ".SS").replace(".XSHE", ".SZ")


def _norm_freq(freq):
    freq = (freq or "1d").lower()
    if freq in ("daily", "day", "1d"):
        return "1d"
    if freq in ("min", "minute", "1m"):
        return "1m"
    return freq
```
`_build_history_wide(bars, jq_codes, field)`：
```python
def _build_history_wide(bars, jq_codes, field):
    """把 history_bars_batch 结果（{jq_code: np.struct}）组装为宽表。
    index=datetime，columns=PTrade 码，值为 field。"""
    out = {}
    for code in jq_codes:
        arr = bars.get(code)
        if arr is None or len(arr) == 0:
            continue
        times = pd.to_datetime(np.asarray(arr["datetime"]).astype(str),
                               format="%Y%m%d%H%M%S")
        actual = "total_turnover" if field == "money" else field
        vals = np.asarray(arr[actual]) if actual in arr.dtype.names \
            else np.full(len(arr), np.nan)
        out[_to_pt(code)] = pd.Series(vals, index=times)
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out).sort_index()
```
`get_history`：
```python
def get_history(count, frequency, field, security_list=None, include=True, fq="pre"):
    """PTrade get_history：单/多标的宽表。security_list 缺省用 _UNIVERSE。"""
    env = Environment.get_instance()
    if security_list is None:
        codes = list(_UNIVERSE)
    elif isinstance(security_list, str):
        codes = [security_list]
    else:
        codes = list(security_list)
    jq_codes = [_to_jq(c) for c in codes]
    freq = _norm_freq(frequency)
    end_dt = getattr(env, "trading_dt", None) or pd.Timestamp.now()
    try:
        bars = _history_bars_batch(jq_codes, int(count), freq, [field], end_dt)
    except Exception as e:  # noqa: BLE001
        logger.debug("get_history 批量失败，回退逐只: %s", e)
        bars = {}
        for jc in jq_codes:
            try:
                arr = env.data_source.history_bars(jc, int(count), freq, [field])
                if arr is not None and len(arr):
                    bars[jc] = arr
            except Exception:  # noqa: BLE001
                continue
    df = _build_history_wide(bars, jq_codes, field)
    if include and not df.empty:
        df = df[df.index <= pd.Timestamp(end_dt)]
    return df


def _history_bars_batch(codes, count, freq, fields, end_dt):
    """批量取数（独立函数便于测试 monkeypatch）。"""
    return Environment.get_instance().data_source.history_bars_batch(
        codes, count, freq, fields, end_dt)
```
`run_daily` / `_ptrade_adapt_bar_dict` / `_install_barcache_mod`（镜像 jqcompat 但读本模块 `_DAILY_AT`、mod 名 `rqalpha_mod_ptradebarcache`）：
```python
def run_daily(context, func, time="HH:MM"):
    """PTrade run_daily：time='HH:MM' 或 'every_bar'。回调签名 func(context)。"""
    if time == "every_bar":
        _EVERY_BAR_CALLBACKS.append(func)
        return
    try:
        hh, mm = str(time).split(":")
        hm = (int(hh), int(mm))
    except Exception:
        hm = (9, 31)
    _DAILY_AT.setdefault(hm, []).append(func)


def _ptrade_adapt_bar_dict(bar_dict):
    """rqalpha BarDict → {PTrade码: SecurityUnitData 替身}。"""
    out = {}
    if not bar_dict:
        return out
    for code, bar in (bar_dict.items() if hasattr(bar_dict, "items") else []):
        try:
            out[_to_pt(code)] = types.SimpleNamespace(
                code=_to_pt(code), dt=getattr(bar, "datetime", None),
                open=getattr(bar, "open", None), high=getattr(bar, "high", None),
                low=getattr(bar, "low", None), close=getattr(bar, "close", None),
                price=getattr(bar, "close", None), volume=getattr(bar, "volume", None),
                money=getattr(bar, "total_turnover", None), name=None)
        except Exception:  # noqa: BLE001
            continue
    return out


def _install_barcache_mod():
    if "rqalpha_mod_ptradebarcache" in sys.modules:
        return
    mod = types.ModuleType("rqalpha_mod_ptradebarcache")

    def load_mod():
        from rqalpha.core.events import EVENT
        from rqalpha.environment import Environment
        from rqalpha.interface import AbstractMod

        class _PtradeBarCacheMod(AbstractMod):
            def start_up(self, env, mod_config):
                _base_cfg = getattr(getattr(env, "config", None), "base", None)
                _freq = str(getattr(_base_cfg, "frequency", "1m") or "1m")
                self._is_daily = _freq == "1d"

                def _uctx():
                    return getattr(getattr(env, "user_strategy", None), "user_context", None)

                def _fire(hm_key=None, exclude=None):
                    uctx = _uctx()
                    items = [(hm_key, _DAILY_AT.get(hm_key, []))] if hm_key is not None \
                        else list(_DAILY_AT.items())
                    for hm, cbs in items:
                        if exclude is not None and hm == exclude:
                            continue
                        for cb in list(cbs):
                            try:
                                cb(uctx)
                            except Exception as e:  # noqa: BLE001
                                logger.warning("daily_at(%s) 回调异常: %s", hm, e)

                def _on_bar(event):
                    uctx = _uctx()
                    for cb in list(_EVERY_BAR_CALLBACKS):
                        try:
                            cb(uctx)
                        except Exception as e:  # noqa: BLE001
                            logger.debug("every_bar 回调异常: %s", e)
                    if self._is_daily:
                        return
                    dt = getattr(env, "trading_dt", None)
                    hm = (dt.hour, dt.minute) if dt is not None else None
                    if hm is not None:
                        for cb in list(_DAILY_AT.get(hm, [])):
                            try:
                                cb(uctx)
                            except Exception as e:  # noqa: BLE001
                                logger.warning("daily_at(%s) 回调异常: %s", hm, e)

                env.event_bus.add_listener(EVENT.BAR, _on_bar)
                if self._is_daily:
                    env.event_bus.add_listener(
                        EVENT.BEFORE_TRADING, lambda e: _fire((9, 31)))
                    env.event_bus.add_listener(
                        EVENT.AFTER_TRADING, lambda e: _fire(None, exclude=(9, 31)))

            def tear_down(self, *args):
                return

        return _PtradeBarCacheMod()

    mod.load_mod = load_mod
    mod.__config__ = {"base": {}, "mod": {}, "extra": {}}
    sys.modules["rqalpha_mod_ptradebarcache"] = mod
```
`_patch_rqalpha_objects()`（StrategyContext.blotter + Portfolio.portfolio_value）：
```python
def _patch_rqalpha_objects():
    try:
        from rqalpha.core.strategy_context import StrategyContext
        from rqalpha.portfolio import Portfolio
        from rqalpha.environment import Environment
    except Exception:  # noqa: BLE001
        return
    if not hasattr(StrategyContext, "blotter"):
        def _blotter(self):
            env = Environment.get_instance()
            return types.SimpleNamespace(current_dt=getattr(env, "calendar_dt", None))
        StrategyContext.blotter = property(_blotter)
    if not hasattr(Portfolio, "portfolio_value"):
        Portfolio.portfolio_value = property(lambda self: self.total_value)
```
`install_ptradecompat(universe, names=None, benchmark="510300.SS", list_dates=None)`：
```python
def install_ptradecompat(universe, names=None, benchmark="510300.SS", list_dates=None):
    global _UNIVERSE, _NAMES, _BENCHMARK
    _UNIVERSE = [str(c) for c in universe]           # JQ 码
    _NAMES = dict(names or {})                       # JQ 码 -> name（strategy 侧转 PTrade）
    _BENCHMARK = benchmark
    _DAILY_AT.clear()
    _EVERY_BAR_CALLBACKS.clear()
    from rqalpha.api import register_api
    register_api("get_history", get_history)
    register_api("run_daily", run_daily)
    register_api("order", order)
    register_api("get_position", get_position)
    register_api("get_positions", get_positions)
    register_api("get_trading_day", get_trading_day)
    register_api("get_trade_days", get_trade_days)
    register_api("set_universe", set_universe)
    register_api("get_stock_status", get_stock_status)
    register_api("get_stock_name", get_stock_name)
    register_api("get_market_list", get_market_list)
    register_api("get_market_detail", get_market_detail)
    register_api("set_benchmark", set_benchmark)
    register_api("set_commission", set_commission)
    register_api("set_slippage", set_slippage)
    register_api("log", log)
    register_api("_ptrade_adapt_bar_dict", _ptrade_adapt_bar_dict)
    _patch_rqalpha_objects()
    _install_barcache_mod()
```
其余 API（`order/get_position/get_positions/get_trading_day/get_trade_days/set_universe/get_stock_status/get_stock_name/get_market_list/get_market_detail/set_benchmark/set_commission/set_slippage/log`）在 Task 3 实现；本 Task 先放占位（抛 NotImplementedError）让骨架编译、调度测试通过。

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradecompat.py -q`
Expected: PASS（4 用例）

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/ptradecompat.py backend/tests/quant/test_ptradecompat.py
git commit -m "feat(quant): ptradecompat 骨架——代码转换/get_history 宽表/调度注册/bar_dict 适配"
```

---
### Task 3: ptradecompat 订单/持仓/停牌/名称/市场 API

**Files:**
- Modify: `backend/app/quant/ptradecompat.py`
- Test: `backend/tests/quant/test_ptradecompat.py`（追加）

**Interfaces:**
- Produces:
  - `order(code, amount)`（转 rqalpha `order`）
  - `get_position(code)` / `get_positions()`（PTrade 键，字段 amount/enable_amount/cost_basis/last_sale_price）
  - `get_trading_day(count)`（count=-1 → 前一交易日）
  - `get_trade_days(start_date=None, end_date=None, count=None)`
  - `set_universe(codes)`（→ rqalpha update_universe，JQ 码）
  - `get_stock_status(codes, query_type='HALT', query_date=None)`（→ `is_suspended`）
  - `get_stock_name(code)`（→ dict{PTrade码: name}）
  - `get_market_list()` / `get_market_detail(mic)`（rqalpha all_instruments）
  - `set_benchmark(code)` / `set_commission(commission_ratio, min_commission, type)` / `set_slippage(slippage)`（存储式 no-op）
  - `log`（Proxy：info/warn/warning/error/debug/notify，输出到 stdout + 可选 sink）

- [ ] **Step 1: Write the failing tests（追加到 test_ptradecompat.py）**

```python
def test_code_conversion_more(_fake_rqalpha):
    pc = _fake_rqalpha
    assert pc._to_jq("511880.SS") == "511880.XSHG"
    assert pc._to_pt("511880.XSHG") == "511880.SS"


def test_position_objects_ptrade_fields(_fake_rqalpha):
    pc = _fake_rqalpha
    from types import SimpleNamespace
    # 模拟 rqalpha PositionProxy 补丁后字段
    proxy = SimpleNamespace(amount=100, enable_amount=100, cost_basis=3.0, last_sale_price=3.1)
    pos = pc._position_view(proxy, "510300.SS")
    assert pos.amount == 100
    assert pos.enable_amount == 100
    assert pos.cost_basis == 3.0
    assert pos.last_sale_price == 3.1


def test_get_trading_day_prev(_fake_rqalpha, monkeypatch):
    pc = _fake_rqalpha
    calls = {}
    def fake_prev(date):
        calls["d"] = date
        return pd.Timestamp("2026-07-17")
    monkeypatch.setattr(pc, "_prev_trading_day", fake_prev)
    assert pc.get_trading_day(-1).strftime("%Y-%m-%d") == "2026-07-17"


def test_set_benchmark_stored(_fake_rqalpha):
    pc = _fake_rqalpha
    pc.set_benchmark("510300.SS")
    assert pc._BENCHMARK == "510300.SS"
```
（`get_trading_day` 内部把「前一交易日」抽成 `_prev_trading_day(date)` 模块函数，测试 monkeypatch。真实实现用 `Environment.get_instance().data_proxy.get_previous_trading_date`。）

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradecompat.py -q`
Expected: FAIL（`_position_view` / `_prev_trading_day` 不存在）

- [ ] **Step 3: Implement the APIs**

参照 jqcompat 同名函数的 rqalpha 取数/补丁模式，在 ptradecompat 中实现：
```python
def _position_view(pos, pt_code):
    """包装 rqalpha 持仓为 PTrade 字段视图（空仓返回 0 占位）。"""
    return types.SimpleNamespace(
        amount=float(getattr(pos, "amount", 0) or 0),
        enable_amount=float(getattr(pos, "enable_amount", 0) or 0),
        cost_basis=float(getattr(pos, "cost_basis", 0) or 0),
        last_sale_price=float(getattr(pos, "last_sale_price", 0) or 0),
        sid=pt_code, security=pt_code)


def get_position(code):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    jq = _to_jq(code)
    try:
        pos = env.portfolio.get_position(jq)
    except Exception:  # noqa: BLE001
        pos = None
    if pos is None:
        return _position_view(None, code)
    # rqalpha PositionProxy → PTrade 字段
    return _position_view(types.SimpleNamespace(
        amount=getattr(pos, "amount", 0) or 0,
        enable_amount=getattr(pos, "enable_amount", 0) or 0,
        cost_basis=getattr(pos, "cost_basis", 0) or 0,
        last_sale_price=getattr(pos, "last_sale_price", 0) or 0), code)


def get_positions():
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    out = {}
    try:
        items = list(env.portfolio.positions.items())
    except Exception:  # noqa: BLE001
        items = []
    for jq, pos in items:
        if float(getattr(pos, "amount", 0) or 0) > 0:
            out[_to_pt(jq)] = _position_view(types.SimpleNamespace(
                amount=getattr(pos, "amount", 0) or 0,
                enable_amount=getattr(pos, "enable_amount", 0) or 0,
                cost_basis=getattr(pos, "cost_basis", 0) or 0,
                last_sale_price=getattr(pos, "last_sale_price", 0) or 0), _to_pt(jq))
    return out


def order(code, amount):
    from rqalpha.api import order as rq_order
    return rq_order(_to_jq(code), int(amount))


def _prev_trading_day(date):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    try:
        return pd.Timestamp(env.data_proxy.get_previous_trading_date(date))
    except Exception:  # noqa: BLE001
        return pd.Timestamp(date) - pd.Timedelta(days=1)


def get_trading_day(count=-1):
    """PTrade get_trading_day(count)：count=-1 返回前一交易日。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    today = pd.Timestamp(getattr(env, "trading_dt", None) or pd.Timestamp.now())
    if count == -1:
        return _prev_trading_day(today.date())
    if count == 1:
        return today.normalize()
    if count > 1:
        cal = env.data_proxy.get_trading_calendar()
        idx = cal.searchsorted(today.normalize())
        return list(cal[max(0, idx - count + 1):idx + 1])[-1]
    return today.normalize()


def get_trade_days(start_date=None, end_date=None, count=None):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    cal = env.data_proxy.get_trading_calendar()
    if end_date is not None:
        cal = cal[cal <= pd.Timestamp(end_date)]
    if start_date is not None:
        cal = cal[cal >= pd.Timestamp(start_date)]
    if count is not None:
        cal = cal[-int(count):]
    return list(cal)


def set_universe(codes):
    from rqalpha.api import update_universe
    if isinstance(codes, str):
        codes = [codes]
    update_universe([_to_jq(c) for c in codes])


def get_stock_status(codes, query_type="HALT", query_date=None):
    """停牌检测：HALT → {PTrade码: 是否停牌}。失败返回空（策略容错为不判定）。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    if isinstance(codes, str):
        codes = [codes]
    out = {}
    for c in codes:
        try:
            out[c] = bool(env.data_proxy.is_suspended(_to_jq(c), query_date))
        except Exception:  # noqa: BLE001
            continue
    return out


def get_stock_name(code):
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    try:
        instr = env.data_proxy.instruments(_to_jq(code))
        return {code: getattr(instr, "symbol", None) or code}
    except Exception:  # noqa: BLE001
        return {code: code}


def get_market_list():
    """PTrade get_market_list：单行 'ALL' 市场（配合 get_market_detail 全市场枚举）。"""
    return pd.DataFrame([{"finance_mic": "ALL"}])


def get_market_detail(mic):
    """全市场基金表：rqalpha all_instruments(type='etf') → prod_code(PTrade)/prod_name。"""
    from rqalpha.environment import Environment
    env = Environment.get_instance()
    rows = []
    try:
        df = env.data_proxy.all_instruments(type="etf")
    except Exception:  # noqa: BLE001
        df = None
    if df is None or df.empty:
        return pd.DataFrame()
    for _, r in df.iterrows():
        jq = str(r.get("order_book_id", ""))
        if not jq:
            continue
        rows.append({"prod_code": _to_pt(jq), "prod_name": str(r.get("symbol", "") or jq)})
    return pd.DataFrame(rows)


def set_benchmark(code):
    global _BENCHMARK
    _BENCHMARK = code


def set_commission(commission_ratio=None, min_commission=None, type=None, **kw):  # noqa: A002
    """PTrade 佣金（回测经 rqalpha 配置生效，此处存储式 no-op）。"""
    return None


def set_slippage(slippage=0.0):
    """PTrade 滑点（回测经 rqalpha 配置生效，此处存储式 no-op）。"""
    return None


class _LogProxy:
    _levels = {"debug": 0, "info": 1, "warn": 2, "error": 3}
    _cur = 1

    def set_level(self, module, level):
        self._cur = self._levels.get(level, 1)

    def _emit(self, msg):
        print("[PTRADE] %s" % msg)

    def info(self, msg):    self._emit(msg)
    def warn(self, msg):    self._emit("[WARN] %s" % msg)
    def warning(self, msg): self.warn(msg)
    def error(self, msg):   self._emit("[ERROR] %s" % msg)
    def debug(self, msg):   self._emit("[DEBUG] %s" % msg)
    def notify(self, msg):  self._emit("[NOTIFY] %s" % msg)


log = _LogProxy()
```
同时在 `install_ptradecompat` 里把上面函数全部 `register_api`（占位被替换）。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradecompat.py -q`
Expected: PASS

- [ ] **Step 5: ruff + commit**

```bash
cd backend && uv run --extra dev ruff check app/quant/ptradecompat.py
git add app/quant/ptradecompat.py tests/quant/test_ptradecompat.py
git commit -m "feat(quant): ptradecompat 订单/持仓/停牌/名称/市场枚举 API"
```

---
### Task 4: rqalpha_bridge.run_ptrade_backtest + run_ptrade_rqalpha.py

**Files:**
- Modify: `backend/app/quant/rqalpha_bridge.py`（新增 `run_ptrade_backtest` + `_run_ptrade_backtest_inner`）
- Create: `backend/scripts/run_ptrade_rqalpha.py`

**Interfaces:**
- Consumes: `ptradecompat.install_ptradecompat`、`JqDataSource`（复用）、`ptradecompat._install_barcache_mod`
- Produces: `run_ptrade_backtest(strategy_path, params, ...) -> dict{run_id, metrics, trades_csv, equity_csv, n_trades, final_equity}`
- 依赖 Task 1 的策略文件（含 `set_benchmark('510300.SS')`、`.SS/.SZ` 池子）

- [ ] **Step 1: Write the runner script（同时充当冒烟入口）**

`backend/scripts/run_ptrade_rqalpha.py`（镜像 `run_jq_rqalpha.py`）：
```python
#!/usr/bin/env python3
"""跑 rqalpha 版 wufu v5.4 双持仓 ptrade 策略，导出 trades/equity。

用法:
  python scripts/run_ptrade_rqalpha.py [--start 2026-04-01] [--end 2026-08-11] \
      [--strategy .../wufu-v5.4-dual-adapt.ptrade.py] [--out data/quant_sim/ptradedual] \
      [--cash 100000] [--fee 0.0001] [--slippage 0.0001]
"""
import argparse
import os
import sys
import warnings

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from app.quant.rqalpha_bridge import run_ptrade_backtest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.join(BACKEND, "tests", "fixtures", "dual_v54")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-08-11")
    ap.add_argument("--strategy", default=os.path.join(REPO, "wufu-v5.4-dual-adapt.ptrade.py"))
    ap.add_argument("--out", default=os.path.join("data", "quant_sim", "ptradedual"))
    ap.add_argument("--cash", type=float, default=100000.0)
    ap.add_argument("--fee", type=float, default=0.0001)
    ap.add_argument("--slippage", type=float, default=0.0001)
    ap.add_argument("--minute_cache_cap", type=int, default=800)
    ap.add_argument("--log_level", default="error")
    args = ap.parse_args()

    params = {
        "start": args.start,
        "end": args.end,
        "capital": args.cash,
        "fee": args.fee,
        "slippage": args.slippage,
        "minute_cache_cap": args.minute_cache_cap,
        "log_level": args.log_level,
        "out_dir": os.path.abspath(args.out),
        "strategy_id": "wufu-v5.4-dual-adapt-ptrade",
    }
    res = run_ptrade_backtest(args.strategy, params)
    if "error" in res:
        print("ERROR:", res["error"])
        sys.exit(1)
    print("trades_csv:", res.get("trades_csv"))
    print("equity_csv:", res.get("equity_csv"))
    print("n_trades:", res.get("n_trades"))
    print("final_equity:", res.get("final_equity"))
    print("metrics:", res.get("metrics"))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 实现 run_ptrade_backtest**

在 `rqalpha_bridge.py` 中新增，结构镜像 `run_jq_backtest`（复用 `_BundleProvider`/`JqDataSource`/`_load_etf_universe`/`_EXTRA_INDEX_CODES`/`_minute_coverage_warning`/`_extract_metrics` 等），差异点：
1. 读取策略源码后，用 `.SS/.SZ` 正则提取固定池：`_re.findall(r"\b(\d{6}\.(?:SS|SZ))\b", strategy_text)`，把提取的 PTrade 码 `_pt_to_jq()` 转 JQ 用于 dm 预载与 universe。
2. 基准：从源码 `set_benchmark\('(\d{6}\.(?:SS|SZ))'\)` 提取（默认 `510300.SS`），转 JQ。
3. 组装 rqalpha 源码前做源重写：
   - `def initialize(context):` 存在 → 注入 `init = initialize`（若策略未定义 `def init(`）；
   - 注入 update_universe（fixed_pools 的 JQ 码）；
   - 注入钩子桥接（rqalpha 钩子名 → PTrade 钩子名 + bar_dict 适配）：
```python
inject_hooks = (
    "\ndef before_trading(context, bar_dict):\n"
    "    data = _ptrade_adapt_bar_dict(bar_dict)\n"
    "    before_trading_start(context, data)\n"
    "\ndef handle_bar(context, bar_dict):\n"
    "    data = _ptrade_adapt_bar_dict(bar_dict)\n"
    "    handle_data(context, data)\n"
    "\ndef after_trading(context):\n"
    "    after_trading_end(context)\n"
)
strategy_code += inject_hooks
```
   仅当策略源码未定义对应 rqalpha 原生钩子时追加（`not _re.search(r"def handle_bar\s*\(", strategy_text)` 等）。
4. config：mod 启用 `ptradebarcache`（替代 `jqbarcache`）、`quantbridge`/`quantlive` 保留；`benchmark` 用 JQ 码；`frequency="1m"`。
5. `install_ptradecompat(valid_universe, names=etf_names, benchmark=benchmark_jq, list_dates=etf_list_dates)`（内部 `_install_barcache_mod` 已注册 ptradebarcache）。
6. `rq_run(config, source_code=strategy_code)` 后 `_extract_metrics/_extract_equity/_extract_trades`，写 out_dir 下 trades.csv/equity.csv，返回 metrics。
7. 关键：`_run_ptrade_backtest_inner` 与 JQ 版共用 dm 预载/offline/区间收敛，但注意 **ptrade 策略内部 g.* 池是 PTrade 码**，`_load_etf_universe` 返回 JQ 码 → `install_ptradecompat` 的 `_UNIVERSE`（JQ 码）会被 `get_history(security_list=None)` 用；而策略显式传 `security_list`，`_UNIVERSE` 仅兜底。

参考 `run_jq_backtest` 的完整流程（`_data_start` 提前 250 天、`dm.preload_daily()`、`_refresh_codes` 补齐、`_avail` 区间收敛、`dm._offline=True` try/finally 恢复），`run_ptrade_backtest` 逐一复制。

- [ ] **Step 3: 冒烟运行**

Run（窗口先缩小到 04-01~04-30 快速验证链路通）：
```bash
cd backend && uv run --extra dev python scripts/run_ptrade_rqalpha.py --start 2026-04-01 --end 2026-04-30 --out /tmp/ptradedual_smoke
```
Expected: 无 ERROR，打印 trades_csv/equity_csv/final_equity。若报错，按 systematic-debugging 排查（重点：`.SS` 提取、钩子桥接、`_ptrade_adapt_bar_dict`）。

- [ ] **Step 4: 完整窗口跑通 + commit**

```bash
cd backend && uv run --extra dev python scripts/run_ptrade_rqalpha.py --start 2026-04-01 --end 2026-08-11 --out /tmp/ptradedual_full
```
Expected: 正常出 metrics。随后：
```bash
git add app/quant/rqalpha_bridge.py scripts/run_ptrade_rqalpha.py
git commit -m "feat(quant): run_ptrade_backtest + run_ptrade_rqalpha.py——ptrade 策略 rqalpha 回测链路"
```

---
### Task 5: 回测对齐验证（核心门禁）

**Files:**
- Create: `backend/tests/quant/test_ptrade_vs_jq_alignment.py`（integration marker）

**Interfaces:**
- Consumes: `run_jq_backtest` + `run_ptrade_backtest` 输出（trades.csv/equity.csv）

- [ ] **Step 1: 写对齐测试（integration，默认 skip，显式跑）**

```python
"""ptrade 双持仓 vs jq 双持仓 rqalpha 回测对齐：收益/逐日净值/成交一致性。"""
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

BACKEND = Path(__file__).parent.parent.parent
RUNNER = BACKEND / "scripts" / "run_ptrade_rqalpha.py"
JQ_RUNNER = BACKEND / "scripts" / "run_jq_rqalpha.py"
PTRADE = BACKEND / "tests" / "fixtures" / "dual_v54" / "wufu-v5.4-dual-adapt.ptrade.py"
JQ = BACKEND / "tests" / "fixtures" / "dual_v54" / "wufu-v5.4-dual-adapt.py"
START, END, CASH, FEE, SLIP = "2026-04-01", "2026-08-11", "100000", "0.0001", "0.0001"


@pytest.mark.integration
@pytest.mark.skipif(not PTRADE.exists() or not JQ.exists(), reason="fixture 缺失")
def test_alignment_full_window(tmp_path):
    jq_out = tmp_path / "jq"
    pt_out = tmp_path / "pt"
    env = dict(os.environ, PYTHONPATH=str(BACKEND))
    subprocess.run([sys.executable, str(JQ_RUNNER),
                    "--start", START, "--end", END, "--strategy", str(JQ),
                    "--out", str(jq_out), "--cash", CASH, "--fee", FEE, "--slippage", SLIP],
                   env=env, check=True, capture_output=True, timeout=1800)
    subprocess.run([sys.executable, str(RUNNER),
                    "--start", START, "--end", END, "--strategy", str(PTRADE),
                    "--out", str(pt_out), "--cash", CASH, "--fee", FEE, "--slippage", SLIP],
                   env=env, check=True, capture_output=True, timeout=1800)
    jq_eq = pd.read_csv(jq_out / "equity.csv", parse_dates=["date"]).set_index("date")
    pt_eq = pd.read_csv(pt_out / "equity.csv", parse_dates=["date"]).set_index("date")
    common = jq_eq.index.intersection(pt_eq.index)
    assert len(common) > 20
    jq_ret = jq_eq.loc[common, "total_returns"] if "total_returns" in jq_eq else _returns(jq_eq)
    pt_ret = pt_eq.loc[common, "total_returns"] if "total_returns" in pt_eq else _returns(pt_eq)
    diff = (pt_ret - jq_ret).abs()
    assert diff.max() <= 0.0005, "逐日净值差 >0.05%%: max=%.4f" % diff.max()
    assert abs(pt_ret.iloc[-1] - jq_ret.iloc[-1]) <= 0.0005


def _returns(eq):
    col = [c for c in ("total_returns", "returns", "unit_net_value") if c in eq.columns][0]
    s = eq[col]
    return s / s.iloc[0] - 1 if col == "unit_net_value" else s
```
（`_returns` 兜底不同 equity.csv 列名，实际列名以 `_extract_equity` 输出为准，若列名不同在实现时按实际调整。）

- [ ] **Step 2: 跑对齐并修复差异**

Run: `cd backend && uv run --extra dev pytest -m integration tests/quant/test_ptrade_vs_jq_alignment.py -q -s`
Expected: 逐日净值差 ≤0.05%。若超差，用 `scripts/diff_jq_vs_local.py` 口径逐日比对 trades，定位差异点（常见：13:10 触发时刻、`_ptrade_adapt_bar_dict` 的 last_price、涨跌停判定字段、成交额字段映射 money/total_turnover），修到对齐。

- [ ] **Step 3: Commit**

```bash
git add tests/quant/test_ptrade_vs_jq_alignment.py
git commit -m "test(quant): ptrade 双持仓 vs jq 双持仓 rqalpha 回测对齐门禁"
```

---
### Task 6: ptradeengine context/portfolio 别名

**Files:**
- Create: `backend/app/quant/ptradeengine/__init__.py`、`backend/app/quant/ptradeengine/context.py`
- Test: `backend/tests/quant/test_ptradeengine.py`

**Interfaces:**
- Produces: `PtradePosition`（别名 enable_amount/cost_basis/last_sale_price）、`PtradePortfolio`（别名 portfolio_value）、`PtradeContext`（含 `blotter.current_dt`、`_code_conv`）
- `_code_conv`：`(to_engine(code), to_pt(code))`，供 runner 边界使用

- [ ] **Step 1: Write the failing test**

`backend/tests/quant/test_ptradeengine.py`：
```python
"""ptradeengine 本地引擎：context/portfolio 别名、代码转换。"""
import sys
import types

from app.quant.ptradeengine.context import PtradeContext, PtradePortfolio, PtradePosition, ptrade_code_conv


def test_position_ptrade_aliases():
    p = PtradePosition(amount=100, avg_cost=3.0, price=3.1)
    assert p.enable_amount == 100
    assert p.cost_basis == 3.0
    assert p.last_sale_price == 3.1
    assert p.total_amount == 100


def test_portfolio_ptrade_alias():
    pf = PtradePortfolio(cash=10000.0)
    pos = PtradePosition(amount=100, avg_cost=3.0, price=3.1)
    pf.positions["510300.SS"] = pos
    assert pf.portfolio_value == pf.total_value == 10000.0 + 310.0


def test_context_blotter():
    import pandas as pd
    ctx = PtradeContext()
    ctx.current_dt = pd.Timestamp("2026-07-10 13:10")
    assert ctx.blotter.current_dt == ctx.current_dt


def test_code_conv():
    to_engine, to_pt = ptrade_code_conv()
    assert to_engine("510300.SS") == "510300.XSHG"
    assert to_pt("510300.XSHG") == "510300.SS"
    assert to_engine("510300.XSHG") == "510300.XSHG"  # jq 码幂等
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine.py -q`
Expected: FAIL（module not found）

- [ ] **Step 3: Implement context.py**

```python
"""PTrade 兼容层 - context / g / Position / Portfolio（镜像 jqengine/engine/jq/context.py）。"""
from __future__ import annotations

import types
from datetime import datetime

from app.quant.jqengine.engine.jq.context import Position
from app.quant.jqengine.engine.jq.portfolio import Portfolio


class PtradePosition(Position):
    """PTrade 字段别名（enable_amount/cost_basis/last_sale_price）。"""

    @property
    def enable_amount(self):
        return self.closeable_amount

    @property
    def cost_basis(self):
        return self.avg_cost

    @property
    def last_sale_price(self):
        return self.price


class PtradePortfolio(Portfolio):
    @property
    def portfolio_value(self):
        return self.total_value


class PtradeContext:
    """PTrade context 子集：blotter.current_dt / portfolio / g / _code_conv。"""

    def __init__(self):
        self.current_dt = None
        self.previous_date = None
        self.universe = []
        self.g = types.SimpleNamespace()
        self.portfolio = None
        self.run_params = types.SimpleNamespace(type="backtest")
        self._code_conv = ptrade_code_conv()

    @property
    def blotter(self):
        return types.SimpleNamespace(current_dt=self.current_dt)


def ptrade_code_conv():
    """返回 (to_engine, to_pt)。to_engine 对已是引擎码(JQ)的输入幂等。"""
    def to_engine(code):
        s = str(code)
        return s.replace(".SS", ".XSHG").replace(".SZ", ".XSHE")

    def to_pt(code):
        s = str(code)
        return s.replace(".XSHG", ".SS").replace(".XSHE", ".SZ")

    return to_engine, to_pt
```
`__init__.py`：`from . import context, ptrade_api, ptrade_loader`（api/loader 在后续 Task 建好后可先留空文件占位让 import 不炸——本 Task 先在 `__init__.py` 只 `from . import context`）。

- [ ] **Step 4: Run test to verify it passes + ruff + commit**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine.py -q && uv run --extra dev ruff check app/quant/ptradeengine
git add app/quant/ptradeengine tests/quant/test_ptradeengine.py
git commit -m "feat(quant): ptradeengine context/portfolio PTrade 别名 + 代码转换"
```

---
### Task 7: ptradeengine ptrade_api（本地引擎 PTrade API）

**Files:**
- Create: `backend/app/quant/ptradeengine/ptrade_api.py`
- Modify: `backend/app/quant/ptradeengine/__init__.py`
- Test: `backend/tests/quant/test_ptradeengine.py`（追加）

**Interfaces:**
- Consumes: `PtradeContext/PtradePortfolio/PtradePosition`、共享 DataManager（`_state["manager"]`）、jq `api._live_price`/`order` 撮合口径
- Produces（`_state` 形状与 `jq_api._state` 一致）：
  - `_state["ctx"/"manager"/"fee"/"slippage"/"fee_config"/"daily"/"minute"/"trades"/"minute_prices"/"minute_mode"/"no_buy"/"no_sell"/"log_sink"]`
  - `on_new_day()`、`run_daily(context, func, time)`、`get_history(count, freq, field, security_list, include, fq)`、`get_position/get_positions`、`order(code, amount)`、`set_universe(codes)`、`get_stock_status(HALT)`、`get_stock_name`、`get_market_list/get_market_detail`、`set_benchmark/set_commission/set_slippage`、`log`
  - `build_data_snapshot(ctx)` → `{PTrade码: SimpleNamespace(dt,open,high,low,close,price,volume,money)}`（供 loader 包装 handle_data/before_trading_start 用）

- [ ] **Step 1: Write the failing tests（追加）**

```python
def test_api_state_shape_and_code_domain():
    from app.quant.ptradeengine import ptrade_api as api
    api._reset(None, 0.0001, 0.0001, 100000.0)
    for key in ("ctx", "manager", "fee", "slippage", "fee_config", "daily", "minute",
                "trades", "minute_prices", "minute_mode", "no_buy", "no_sell", "log_sink"):
        assert key in api._state, key
    assert callable(api.on_new_day)


def test_api_run_daily_registers():
    from app.quant.ptradeengine import ptrade_api as api
    api._reset(None, 0.0001, 0.0001, 100000.0)
    calls = []
    def cb(context):
        calls.append(1)
    api.run_daily(None, cb, time="13:10")
    assert (cb, "13:10") in api._state["daily"]


def test_api_order_records_ptrade_code():
    """order 用 PTrade 码，成交 trades 记 PTrade 码，portfolio positions 键 PTrade 码。"""
    from app.quant.ptradeengine import ptrade_api as api
    api._reset(_StubDm(), 0.0001, 0.0001, 100000.0)
    api._state["minute_prices"] = {"510300.SS": 3.0}
    api._state["minute_mode"] = True
    ok = api.order("510300.SS", 1000)
    assert ok
    assert "510300.SS" in api._state["ctx"].portfolio.positions
    assert api._state["trades"][-1]["code"] == "510300.SS"
```
其中 `_StubDm` 提供 `get_minute_price_at`/`fetch` 桩（返回空/None 即可，order 走 minute_prices 快照）。

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine.py -q`
Expected: FAIL（ptrade_api 无 `_reset`/`order`）

- [ ] **Step 3: Implement ptrade_api**

参照 `jqengine/engine/jq/api.py` 的结构与撮合口径（100 股整手、T+1、佣金双边+卖出印花税非ETF、no_buy/no_sell），但 **代码域为 PTrade**、state 键与 runner plumbing 兼容。核心函数（完整实现见 jq api.order 同款逻辑，security 直接使用 PTrade 码，不转换）：
```python
def _reset(manager, fee, slippage, cash):
    global _current_ctx
    ctx = PtradeContext()
    ctx.portfolio = PtradePortfolio(cash)
    ctx._code_conv = ptrade_code_conv()
    _state.update(ctx=ctx, manager=manager, fee=fee, slippage=slippage,
                  fee_config=None, daily=[], minute=[], records=[], trades=[],
                  minute_prices={}, minute_mode=False,
                  no_buy=set(), no_sell=set(), log_sink=None)
    return ctx


def on_new_day():
    ctx = _state.get("ctx")
    if ctx is not None and ctx.portfolio is not None:
        for pos in ctx.portfolio.positions.values():
            pos.today_amount = 0.0


def run_daily(context, func, time="HH:MM"):
    _state["daily"].append((func, str(time)))


def order(security, amount):
    # 与 jq api.order 同口径；security 已是 PTrade 码，直接操作。
    # prices 取自 _state["minute_prices"][security]（PTrade 域）。
    ...


def get_history(count, frequency, field, security_list=None, include=True, fq="pre"):
    # security_list 为 PTrade 码；底层 DataManager 用 JQ 码，取数时
    # conv.to_engine() 转换，返回 DataFrame(index=datetime, columns=PTrade 码)。
    ...


def get_position(security):
    pos = _state["ctx"].portfolio.positions.get(security)
    if pos is None:
        return PtradePosition()
    return pos


def get_positions():
    return {c: p for c, p in _state["ctx"].portfolio.positions.items() if p.amount > 0}


def set_universe(codes):
    if isinstance(codes, str):
        codes = [codes]
    _state["ctx"].universe = list(codes)  # PTrade 域（runner feed 前转引擎码）
```
`get_history` 的取数细节：`dm.fetch("get_daily", conv.to_engine(code), start, end)` / `dm.get_minute(code, end_dt, start)`，规整为宽表；`fq="pre"` 与 jq get_price 同口径（本地日线统一前复权）。`get_stock_status(HALT)` 用 `_state["minute_prices"]`/分钟可得性推导。`get_market_list/detail` 用 `dm.fetch("get_etf_list")` 返回 PTrade 码。`build_data_snapshot(ctx)` 从 `_state["minute_prices"]` + `_state["manager"]` 日线 OHLC 组装快照。

- [ ] **Step 4: Run test to verify it passes + ruff + commit**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine.py -q && uv run --extra dev ruff check app/quant/ptradeengine
git add app/quant/ptradeengine
git commit -m "feat(quant): ptradeengine ptrade_api——PTrade 域订单/行情/持仓 API + state 兼容"
```

---
### Task 8: ptradeengine ptrade_loader + runner 路由 + conv 钩子

**Files:**
- Create: `backend/app/quant/ptradeengine/ptrade_loader.py`
- Modify: `backend/app/quant/simulate/runner.py`
- Test: `backend/tests/quant/test_ptradeengine.py`（追加 loader）+ `backend/tests/quant/test_sim_runner_flavor.py`

**Interfaces:**
- Consumes: `ptrade_api`（_state/daily/trades）、`PtradeContext`
- Produces: `load_strategy(code, manager, fee, slippage, cash) -> StrategyBundle`（含 `init_fn`、`before_trading_start`/`after_trading_end`/`handle_data`（包装为 `(ctx)`）、`daily`/`minute`、`conv`）
- runner 新增：`_load_engine()` 按 flavor 返回 `(api, loader)`；`_seed_universe`/`_strategy_tick`/`_prev_close_dm`/`_revalue_at_close` 用 `ctx._code_conv` 在数据边界转换

- [ ] **Step 1: Write the failing test（loader）**

```python
def test_loader_bundle_hooks_and_conv():
    from app.quant.ptradeengine import ptrade_loader
    code = (
        "def initialize(context):\n"
        "    g.holdings_num = 2\n"
        "    run_daily(context, after, time='13:10')\n"
        "def after(context):\n"
        "    pass\n"
        "def handle_data(context, data):\n"
        "    _set_last_data(data, context)\n"
        "def before_trading_start(context, data):\n"
        "    _set_last_data(data, context)\n"
        "def after_trading_end(context):\n"
        "    pass\n"
    )
    b = ptrade_loader.load_strategy(code, None, 0.0001, 0.0001, 100000.0)
    assert b.before_trading_start is not None
    assert b.after_trading_end is not None
    assert b.handle_data is not None
    assert len(b.daily) == 1 and b.daily[0][1] == "13:10"
    # conv 翻译
    assert b.conv.to_engine("510300.SS") == "510300.XSHG"
```
（loader 需向策略命名空间注入 `run_daily`（3参）、`g`、`context`、`log`、`_set_last_data`/`_cd` 的桩——因为策略文件里定义了它们，但测试代码没定义；实际 loader 只需注入 `run_daily`/`g`/`context`/`log`，策略文件自带 `_set_last_data`。测试代码里去掉对 `_set_last_data` 的依赖，handle_data 直接 `pass`。）

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine.py -q`
Expected: FAIL

- [ ] **Step 3: Implement ptrade_loader**

镜像 `jqengine/engine/jq/loader.py`，命名空间注入 ptrade API（`run_daily`/`order`/`get_history`/`get_position`/`get_positions`/`set_universe`/`get_stock_status`/`get_stock_name`/`get_market_list`/`get_market_detail`/`log`/`g`/`context`），bundle hooks 包装：
```python
def _wrap_with_data(fn):
    def wrapped(ctx):
        data = build_data_snapshot(ctx)
        return fn(ctx, data)
    return wrapped
```
`handle_data`/`before_trading_start` 用 `_wrap_with_data` 包装；`after_trading_end` 原样（单参）。`bundle.conv = ctx._code_conv` 元组。

- [ ] **Step 4: runner 路由 + conv 钩子**

`simulate/runner.py`：
1. `_load_engine()` 改为接受 flavor 参数或按 `get_strategy` 代码嗅探：
```python
def _is_ptrade_strategy(code: str) -> bool:
    return bool(code) and (".SS" in code or ".SZ" in code)


def _load_engine(code: str = ""):
    if _is_ptrade_strategy(code):
        from ..ptradeengine import ptrade_api, ptrade_loader
        return ptrade_api, ptrade_loader
    from ..jqengine.engine.jq import api as jq_api
    from ..jqengine.engine.jq import loader as jq_loader
    return jq_api, jq_loader
```
`_run_strategy_loop` 里 `_load_engine()` 改为 `_load_engine(code)`。
2. `_seed_universe(ctx)`：追加指数码时用 `ctx._code_conv` 转策略域：
```python
conv = getattr(ctx, "_code_conv", None) or (lambda c: c, lambda c: c)
_, to_pt = conv
for c in ("000300.XSHG", "399101.XSHE", "399006.XSHE", "000510.XSHG"):
    pools.append(to_pt(c))
```
（jq 策略 ctx 无 `_code_conv` → identity，行为不变。）
3. `_strategy_tick`：feed watch 转引擎码、prices 键转回 PTrade：
```python
conv = getattr(ctx, "_code_conv", None) or (lambda c: c, lambda c: c)
to_engine, to_pt = conv
watch_engine = [to_engine(c) for c in watch]
prices, bar_dt = feed(dm, watch_engine, now, aux["fresh_frames"])
if prices:
    prices = {to_pt(c): v for c, v in prices.items()}
```
4. `_prev_close_dm(dm, code, today)`：加 `conv=None` 参数，`code = (conv or (lambda c:c,))[0](code)` 再 dm.fetch（保持 cache_key 用原 PTrade 码）。
5. `_revalue_at_close(dm, ctx, state, bar_dt)`：positions 键转引擎码再 dm.get_minute_price_at/fetch。
6. `_restore_portfolio`：state positions 键即引擎内持久化码（ptrade 账户存 PTrade 码），直接重建（无需改）。

- [ ] **Step 5: Run tests + ruff + commit**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine.py tests/quant/test_sim_runner_flavor.py -q
cd backend && uv run --extra dev ruff check app/quant/simulate/runner.py app/quant/ptradeengine
git add app/quant/ptradeengine app/quant/simulate/runner.py tests/quant
git commit -m "feat(quant): ptradeengine loader + simulate/runner ptrade flavor 路由与代码转换钩子"
```

---
### Task 9: 本地引擎对齐验证（模拟盘路径冒烟）

**Files:**
- Create: `backend/tests/quant/test_ptradeengine_local_alignment.py`（integration）

**Interfaces:**
- Consumes: ptrade_loader + runner 驱动 + jq 引擎（同 DataManager）
- 目标：短窗口（如 04-01~04-15）用 runner 的 replay 路径分别跑 jq dual-adapt 与 ptrade dual-adapt，成交序列一致。

- [ ] **Step 1: Write the smoke test**

复用 `simulate/runner._replay_history` 的驱动不易单测，改为**直接驱动 loader** 的轻量对齐：用同一份模拟 DataManager 桩，逐 bar 调用 `bundle.init_fn(ctx)` → `before_trading_start` → 各 run_daily → `handle_data`，jq 与 ptrade 各跑一遍，比较 `trades` 的 `(code, side, amount, dt)` 序列。桩 DataManager 提供 `fetch("get_daily", ...)` / `get_minute` / `get_minute_price_at`（返回固定小数据集），两者共用。
预期：jq 用 `.XSHG` 码、ptrade 用 `.SS/.SZ` 码（归一后比对）。

- [ ] **Step 2: 跑通 + 修复差异 + commit**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine_local_alignment.py -q -s
git add tests/quant/test_ptradeengine_local_alignment.py
git commit -m "test(quant): 本地引擎 jq/ptrade 双持仓成交对齐冒烟"
```

---
## 验证汇总（plan 自检后）

- [ ] `cd backend && uv run --extra dev pytest tests/quant/test_ptrade_strategy_file.py tests/quant/test_ptradecompat.py tests/quant/test_ptradeengine.py -q` 全绿
- [ ] `cd backend && uv run --extra dev ruff check app tests`（仅本次新增/改动文件）
- [ ] `cd backend && uv run --extra dev mypy app/quant/ptradecompat.py app/quant/ptradeengine`（新文件）
- [ ] Task 5 回测对齐：逐日净值差 ≤0.05%
- [ ] Task 9 本地引擎对齐：成交一致
