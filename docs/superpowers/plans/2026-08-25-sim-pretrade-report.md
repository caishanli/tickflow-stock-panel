# 模拟盘预买卖报告实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 克隆模拟盘 d092ad90 为新账户（镜像当前状态），新账户策略在每天 11:20/13:01 发预买卖报告（预测 13:10 调仓，卖出按"最可能"排前3）到 log+钉钉。

**Architecture:** 策略文件副本内置两个 `run_daily` 预报告任务，复用现有 `get_final_ranked_etfs` 加 quiet 参数实现同口径预测；`log.notify` 自动落 sim_logs+推钉钉。`run_quant_sim.py` 加 `--clone-from` 克隆账户配置与 sim_state。

**Tech Stack:** Python / jqengine 策略 DSL / SQLite(quant.db) / pytest

## Global Constraints

- 后端命令一律 `cd backend && uv run --extra dev ...`（dev 依赖不在基础 venv）
- quant.db 路径用 `CONFIG.db_path`，禁止硬编码 `"data/quant.db"`
- 不改动 `wufu-v5.4-ding.py` 原文件、runner、db schema
- 策略文件风格：聚宽 jq DSL，无类型注解；测试仿 `backend/tests/quant/test_wufu_ding_strategy.py` 的 exec-stub 模式
- spec：`docs/superpowers/specs/2026-08-25-sim-pretrade-report-design.md`

---

### Task 1: 策略副本 wufu-v5.4-ding-report.py（quiet 排名 + 预报告）

**Files:**
- Create: `data/quant_strategies/wufu-v5.4-ding-report.py`（部署用，data/ 不入库）
- Create: `backend/tests/fixtures/wufu_v54/wufu-v5.4-ding-report.py`（入库的测试快照，内容与上面完全一致）
- Test: `backend/tests/quant/test_wufu_ding_report_strategy.py`

**Interfaces:**
- Produces: `get_final_ranked_etfs(context, quiet=False)`（原签名加参）；`pre_trade_report(context)`；`_build_pre_trade_message(context, tag, ranked)`；`_pre_report_target_codes(context, ranked)`；`PRE_REPORT_TOP_SELLS=3`
- 引擎事实（已核实）：`_fire_session` 的去重 key 是 `(id(func), str(task_time))`——同一函数注册 11:20 与 13:01 各触发一次；`in_trading` 含 11:20 边界，11:20 bar 会处理。

- [ ] **Step 1: 写失败测试**

创建 `backend/tests/quant/test_wufu_ding_report_strategy.py`：

```python
"""wufu-v5.4-ding-report 策略验证：语法 + 预买卖报告组装/排序/守卫。"""
import py_compile
import sys
import types
from datetime import datetime
from pathlib import Path

STRATEGY = Path(__file__).parent.parent / "fixtures" / "wufu_v54" / "wufu-v5.4-ding-report.py"


def test_strategy_compiles():
    assert STRATEGY.exists()
    py_compile.compile(str(STRATEGY), doraise=True)


def test_strategy_registers_two_pre_reports_and_quiet_call():
    src = STRATEGY.read_text(encoding="utf-8")
    assert "run_daily(pre_trade_report, time='11:20')" in src
    assert "run_daily(pre_trade_report, time='13:01')" in src
    assert "get_final_ranked_etfs(context, quiet=True)" in src
    assert src.count("if not quiet:") >= 4  # 头部2行info + 保护告警 + 全量表×2处


class _FakeLog:
    def __init__(self):
        self.notifies, self.infos, self.warnings = [], [], []

    def info(self, msg):
        self.infos.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def error(self, msg):
        pass

    def set_level(self, *a):
        pass

    def notify(self, msg):
        self.notifies.append(msg)


def _load_strategy():
    fake_jq = types.ModuleType("jqdata")  # 规避 from jqdata import *
    sys.modules["jqdata"] = fake_jq
    ns = {"__name__": "wufu_ding_report_test", "log": _FakeLog(),
          "g": types.SimpleNamespace(), "get_trade_days": lambda **k: []}
    exec(compile(STRATEGY.read_text(encoding="utf-8"), str(STRATEGY), "exec"), ns)
    return ns


def _m(etf, name, score):
    return {"etf": etf, "etf_name": name, "momentum_score": score}


def _make_ctx(positions, dt=datetime(2026, 8, 25, 13, 1)):
    c = types.SimpleNamespace()
    c.positions = {code: types.SimpleNamespace(total_amount=amt)
                   for code, amt in positions.items()}
    c.current_dt = dt
    port = types.SimpleNamespace(positions=c.positions)
    ctx = types.SimpleNamespace(portfolio=port, current_dt=c.current_dt)
    return ctx


NAMES = {"A.XSHG": "甲ETF", "B.XSHG": "乙ETF", "C.XSHE": "丙ETF", "D.XSHG": "丁ETF",
         "E.XSHE": "戊ETF", "F.XSHE": "己ETF", "511880.XSHG": "银华日利"}
FULL = [_m("A.XSHG", "甲ETF", 2.0), _m("B.XSHG", "乙ETF", 1.8), _m("C.XSHE", "丙ETF", 1.6),
        _m("D.XSHG", "丁ETF", 1.4), _m("E.XSHE", "戊ETF", 1.2), _m("F.XSHE", "己ETF", 1.0)]
ASSESS = {"A.XSHG", "B.XSHG", "C.XSHE", "D.XSHG", "E.XSHE", "F.XSHE"}


def _prep(ns, ranked_full=None, assessed=None, holdings_num=3,
          defensive_ok=True, pool=("A.XSHG", "B.XSHG", "C.XSHE", "E.XSHE", "F.XSHE")):
    g = ns["g"]
    g.holdings_num = holdings_num
    g.is_a_share_weak = False
    g.merged_etf_pool = list(pool)
    g.defensive_etf = "511880.XSHG"
    g.ranked_candidates_full = FULL if ranked_full is None else ranked_full
    g._assessed_codes = ASSESS if assessed is None else assessed
    ns["get_security_name"] = lambda c: NAMES.get(c, c)
    ns["check_defensive_etf_available"] = lambda ctx: defensive_ok


def test_report_predicts_buy_and_sell_order():
    ns = _load_strategy()
    _prep(ns)
    ctx = _make_ctx({"C.XSHE": 1000, "E.XSHE": 2000})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", FULL[:3])
    assert msg.startswith("📋 预买卖报告 08-25 13:01")
    assert "🟢 大A正常期 | 池5只" in msg
    assert "📥 预计买入：A 甲ETF → B 乙ETF" in msg          # 目标 A,B,C 减持仓 C
    assert "📤 预计卖出（最可能前1）：" in msg
    assert "1️⃣ E 戊ETF（排名5/6，动量1.2000）" in msg       # E 持仓不在目标


def test_report_sell_worst_rank_first_truncates_three():
    ns = _load_strategy()
    _prep(ns, holdings_num=1)                                # 目标仅 A
    ctx = _make_ctx({"C.XSHE": 100, "D.XSHG": 200, "E.XSHE": 300, "F.XSHE": 400})
    msg = ns["_build_pre_trade_message"](ctx, "11:20", [FULL[0]])
    assert "📤 预计卖出（最可能前3）：" in msg
    lines = [ln for ln in msg.splitlines() if ln[:1].isdigit() or ln.startswith(("1️⃣", "2️⃣", "3️⃣"))]
    assert len(lines) == 3
    assert "3️⃣ D 丁ETF" in lines[2] and "排名4/6" in lines[2]   # 排名最差者第1：F,E,D
    assert "F 己ETF" in lines[0] and "排名6/6" in lines[0]
    assert "乙ETF" not in msg                                 # B 未持有，不是卖出候选也不出现


def test_report_no_sell_when_all_in_targets():
    ns = _load_strategy()
    _prep(ns)
    ctx = _make_ctx({"A.XSHG": 1000, "B.XSHG": 2000})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", FULL)
    assert "✅ 持仓全部在目标内（2只），13:10 预计不动" in msg
    assert "📤" not in msg and "📥 预计买入：无" in msg


def test_report_defensive_mode_when_ranked_empty():
    ns = _load_strategy()
    _prep(ns, ranked_full=[], assessed=set())
    ctx = _make_ctx({"C.XSHE": 500, "E.XSHE": 600})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", [])
    assert "🛡️ 排名为空：走防御模式" in msg
    assert "📥 预计买入：511880 银华日利" in msg
    assert "1️⃣ C 丙ETF（未入过滤排名，动量N/A）" in msg       # 无排名者按序列出
    assert "2️⃣ E 戊ETF" in msg


def test_report_unevaluated_holding_listed_separately():
    ns = _load_strategy()
    _prep(ns, assessed=ASSESS - {"F.XSHE"})                  # F 在池内但未参与评估
    ctx = _make_ctx({"C.XSHE": 500, "F.XSHE": 600})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", FULL)
    assert "⚠️ 未参与评估（数据缺失保护，13:10 不会强卖）：F.XSHE 己ETF" in msg
    assert "📤" not in msg                                    # F 不进卖出候选


def test_pre_trade_report_replay_guard_skips():
    ns = _load_strategy()
    _prep(ns)
    ctx = _make_ctx({"A.XSHG": 100}, dt=datetime(2026, 7, 10, 13, 1))  # 历史 dt
    ns["pre_trade_report"](ctx)
    assert not ns["log"].notifies                             # 补跑直接跳过


def test_pre_trade_report_pool_not_ready_skips():
    ns = _load_strategy()
    del ns["g"].__dict__["merged_etf_pool"] if hasattr(ns["g"], "merged_etf_pool") else None
    ns["g"].__dict__.pop("merged_etf_pool", None)
    ctx = _make_ctx({})
    ns["pre_trade_report"](ctx)
    assert not ns["log"].notifies
    assert any("合并池未就绪" in m for m in ns["log"].infos)
```

注意 `_make_ctx` 返回的 ctx 同时满足 `_build_pre_trade_message`（读 `context.portfolio.positions`）与 `pre_trade_report` 守卫（读 `context.current_dt`）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_wufu_ding_report_strategy.py -q`
Expected: FAIL（STRATEGY 文件不存在 / KeyError pre_trade_report）

- [ ] **Step 3: 创建策略副本并修改**

```bash
cp data/quant_strategies/wufu-v5.4-ding.py data/quant_strategies/wufu-v5.4-ding-report.py
cp data/quant_strategies/wufu-v5.4-ding-report.py backend/tests/fixtures/wufu_v54/wufu-v5.4-ding-report.py
```

对 **两份副本都做**以下相同编辑（保持逐字节一致）：

3a. 文件头注释块末尾（第 9 行 `#   静默跳过...不基于残缺排名换仓。` 之后）加一行：

```python
# v5.4-report（2026-08-25）：克隆 wufu-v5.4-ding；每天 11:20/13:01 预买卖报告
#   （预测 13:10 调仓：预计买入 + 最可能卖出前3），log.notify 落库+推钉钉。
```

3b. `initialize` 内，buy_routine 注册行之后插入两行：

```python
    run_daily(pre_trade_report, time='11:20')           # 预买卖报告：午间场（预测 13:10 调仓）
    run_daily(pre_trade_report, time='13:01')           # 预买卖报告：尾盘场（距决策9分钟）
```

3c. `def get_final_ranked_etfs(context):` 改为：

```python
def get_final_ranked_etfs(context, quiet=False):
```

并在其下补一行 docstring：`"""全流程动量排名→候选→结合持仓得最终目标。quiet=True 静默（预报告复用），计算逻辑与非 quiet 完全一致。"""`

3d. 函数内头部两条 log.info 包上 quiet 守卫：

```python
    if not quiet:
        log.info(f"【动量得分计算】使用合并池，合计{len(etf_set)}只ETF")
        log.info(f"【当前状态】{'🔴 大A走弱期' if g.is_a_share_weak else '🟢 大A正常期'}")
```

3e. 「数据缺失保护」块中，把 warning/notify 包守卫（retained.append 保持无条件，预测必须与 13:10 行为一致）：

```python
        name = get_security_name(etf)
        if not quiet:
            log.warning(f"🛡️ 【数据缺失保护】{etf} {name} 在合并池内但未参与今日动量计算（日线/分钟取数失败），保守保留持仓不换仓")
            try:
                log.notify(f"🛡️ 数据缺失保护：{name}({etf}) 今日动量数据缺失，保留持仓不换仓")
            except Exception:
                pass
        retained.append({'etf': etf, 'etf_name': name, 'momentum_score': float('inf')})
```

3f. 两处 `log.info(full_log)`（「无符合条件的ETF」早退分支 与 函数尾部）均改为：

```python
        if not quiet:
            log.info(full_log)
```

说明：函数仍会写 `g._assessed_codes` / `g.ranked_candidates_full`（quiet 也写）——无害且被预报告读取用于排序展示；13:10 正式管线会重新覆盖。

3g. 在 `def reset_daily_flags(context):` 之前插入整段：

```python
# ==================== 预买卖报告（v5.4-report）====================
PRE_REPORT_TOP_SELLS = 3       # 卖出候选最多展示前 3


def pre_trade_report(context):
    """11:20 / 13:01 预买卖报告：预测 13:10 调仓（预计买入 + 最可能卖出前3）。

    直接复用 get_final_ranked_etfs(quiet=True)，与 13:10 正式管线完全同口径
    （含持仓宽容/数据缺失保护）。结果走 log.notify：落 sim_logs 并推钉钉。
    """
    try:
        lag_seconds = (datetime.now() - context.current_dt).total_seconds()
    except Exception:
        lag_seconds = 0.0
    if lag_seconds > 300:
        return  # 历史补跑：不浪费全池计算、不产生无用日志
    if not getattr(g, 'merged_etf_pool', None):
        log.info("【预买卖报告】合并池未就绪，跳过本次预报告")
        return
    tag = context.current_dt.strftime('%H:%M')
    log.info(f"▶️ 【预买卖报告 @{tag}】启动...")
    try:
        from app.quant.jqengine.datasource.manager import get_data_manager
        dm = get_data_manager()
        dm.preload_minute_for_pool(g.merged_etf_pool, context.current_dt)
        log.info(f"📦 【预买卖报告】已预热 {len(g.merged_etf_pool)} 只分钟缓存")
    except Exception as e:
        log.warning(f"【预买卖报告】分钟线预加载失败（回退逐标的取数）: {e}")
    ranked = get_final_ranked_etfs(context, quiet=True)
    msg = _build_pre_trade_message(context, tag, ranked)
    log.notify(msg)
    log.info(f"⏸️ 【预买卖报告 @{tag}】执行完毕！")


def _short_code(code):
    return code.split('.')[0]


def _pre_report_target_codes(context, ranked):
    """与 execute_sell_trades 同口径的目标集推导：前 N 或防御兜底。"""
    if ranked:
        return [m['etf'] for m in ranked[:g.holdings_num]], False
    if check_defensive_etf_available(context):
        return [g.defensive_etf], True
    return [], True


def _build_pre_trade_message(context, tag, ranked):
    """组装预买卖报告文本（纯展示逻辑，可单测）。

    卖出候选排序＝"最可能被卖"优先：无过滤排名者最先，其余按完整过滤排名
    位置从后往前（排名越差越先卖）；截断前 PRE_REPORT_TOP_SELLS。
    """
    holdings = [sec for sec, pos in context.portfolio.positions.items()
                if pos.total_amount > 0]
    regime = '🔴 大A走弱期' if getattr(g, 'is_a_share_weak', False) else '🟢 大A正常期'
    pool_size = len(getattr(g, 'merged_etf_pool', []) or [])
    head = (f"📋 预买卖报告 {context.current_dt.strftime('%m-%d')} {tag}"
            f"（预测 13:10 调仓 | {regime} | 池{pool_size}只）")

    target_codes, defensive_mode = _pre_report_target_codes(context, ranked)
    hold_set = set(holdings)
    target_set = set(target_codes)
    lines = [head]
    if defensive_mode:
        lines.append("🛡️ 排名为空：走防御模式" if target_codes
                     else "🛡️ 排名空且防御ETF不可用：走空仓模式")

    buys = [c for c in target_codes if c not in hold_set]
    if buys:
        buy_strs = [f"{_short_code(c)} {get_security_name(c)}" for c in buys]
        lines.append("📥 预计买入：" + " → ".join(buy_strs))
    else:
        lines.append("📥 预计买入：无")

    full_rank = getattr(g, 'ranked_candidates_full', []) or []
    rank_pos = {m['etf']: i for i, m in enumerate(full_rank)}
    score_of = {}
    for m in full_rank:
        score_of.setdefault(m['etf'], m.get('momentum_score'))
    total = max(len(full_rank), 1)
    assessed = set(getattr(g, '_assessed_codes', []) or [])
    pool_set = set(getattr(g, 'merged_etf_pool', []) or [])

    sells, unevaluated = [], []
    for sec in holdings:
        if sec in target_set:
            continue
        if assessed and sec not in assessed and sec in pool_set:
            unevaluated.append(f"{sec} {get_security_name(sec)}")
            continue
        p = rank_pos.get(sec)
        s = score_of.get(sec)
        pos_str = f"排名{p + 1}/{total}" if p is not None else "未入过滤排名"
        s_str = f"{s:.4f}" if isinstance(s, (int, float)) and s == s else "N/A"
        sells.append((sec, get_security_name(sec), pos_str, s_str, p))

    if sells:
        sells.sort(key=lambda x: (x[4] is not None, -(x[4] if x[4] is not None else 0)))
        n = min(PRE_REPORT_TOP_SELLS, len(sells))
        lines.append(f"📤 预计卖出（最可能前{n}）：")
        emojis = ['1️⃣', '2️⃣', '3️⃣']
        for i, (sec, nm, pos_str, s_str, _p) in enumerate(sells[:PRE_REPORT_TOP_SELLS]):
            lines.append(f"{emojis[i]} {_short_code(sec)} {nm}（{pos_str}，动量{s_str}）")
    elif not unevaluated:
        lines.append(f"✅ 持仓全部在目标内（{len(holdings)}只），13:10 预计不动")

    if unevaluated:
        lines.append(f"⚠️ 未参与评估（数据缺失保护，13:10 不会强卖）：{'、'.join(unevaluated)}")
    return "\n".join(lines)


def reset_daily_flags(context):   # ← 注意：这是原有函数，插入段在其之前，勿重复定义
```

（最后这行只是锚点示意——实际操作是把上面「预买卖报告」整段插在原 `reset_daily_flags` 定义之前，不要动原函数。）

- [ ] **Step 4: 校验两份文件逐字节一致 + 跑测试通过**

Run: `cmp data/quant_strategies/wufu-v5.4-ding-report.py backend/tests/fixtures/wufu_v54/wufu-v5.4-ding-report.py && cd backend && uv run --extra dev pytest tests/quant/test_wufu_ding_report_strategy.py -v`
Expected: cmp 无输出；测试全部 PASS。
再跑回归：`uv run --extra dev pytest tests/quant/test_wufu_ding_strategy.py tests/quant/test_sim_runner_flavor.py -q` Expected: PASS（原策略不受影响）。

- [ ] **Step 5: Commit**

```bash
git add backend/tests/fixtures/wufu_v54/wufu-v5.4-ding-report.py backend/tests/quant/test_wufu_ding_report_strategy.py
git commit -m "feat: wufu-v5.4-report 策略副本——11:20/13:01 预买卖报告（预测13:10调仓，卖前3）"
```

---

### Task 2: run_quant_sim.py 增加 --clone-from

**Files:**
- Modify: `backend/scripts/run_quant_sim.py`
- Test: `backend/tests/quant/test_clone_account.py`

**Interfaces:**
- Consumes: `db.get_sim_account(aid)`、`db.insert_sim_account(account_id, name, capital, stop_loss, status, strategy_id, start_date, frequency)`、`db.update_sim_account(aid, **fields)`、`db.read_sim_state(aid)`（返回含原始 positions_json/stop_loss_log_json 字符串）、`db.upsert_sim_state(account_id, cash, positions_json, net_value, pnl, start_cash, stop_loss_log_json, dt)`
- Produces: CLI `--create --clone-from <aid> [--strategy-id SID(必填)] [--name N(缺省 "{源name}-预报告")] [--capital/--stop-loss/--start-date(缺省继承源值)] [--account-id][--autostart]`

- [ ] **Step 1: 写失败测试**

创建 `backend/tests/quant/test_clone_account.py`：

```python
"""--clone-from 克隆账户：配置镜像 + sim_state 整行镜像。"""
from __future__ import annotations

import json

import pytest

from app.quant import db
from app.quant.config import CONFIG


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    db.init_db(str(db_path))
    return tmp_path


@pytest.fixture
def source_account(tmp_quant):
    db.insert_sim_account("src12345", "五福v5.4-钉钉版", 100000.0, 0.03,
                          "paused", "wufu-src-strategy", "2026-07-10", "minute")
    db.update_sim_account("src12345", dingtalk_enabled=1)
    db.upsert_sim_state("src12345", 139.7262,
                        json.dumps({"159502.XSHE": {"amount": 70800.0, "avg_cost": 1.673}}),
                        116605.7262, 16605.7262, 100000.0, "[]", "2026-08-25 11:19:00")
    return "src12345"


def _run_create(argv):
    import sys as _sys
    from scripts.run_quant_sim import main
    old = _sys.argv
    _sys.argv = ["run_quant_sim.py", *argv]
    try:
        main()
    finally:
        _sys.argv = old


def test_clone_mirrors_config_and_state(source_account, capsys):
    _run_create(["--create", "--clone-from", source_account,
                 "--strategy-id", "wufu-v5.4-ding-report", "--autostart"])
    accounts = {a["id"]: a for a in db.list_sim_accounts()}
    clone_id = next(aid for aid in accounts if aid != source_account)
    clone = accounts[clone_id]
    assert clone["strategy_id"] == "wufu-v5.4-ding-report"
    assert clone["name"] == "五福v5.4-钉钉版-预报告"           # 缺省名 = 源名-预报告
    assert clone["capital"] == 100000.0 and clone["stop_loss"] == 0.03
    assert clone["start_date"] == "2026-07-10" and clone["frequency"] == "minute"
    assert clone["dingtalk_enabled"] == 1
    st = db.read_sim_state(clone_id)
    assert st["cash"] == 139.7262 and st["dt"] == "2026-08-25 11:19:00"
    assert "159502.XSHE" in st["positions"]                    # 持仓镜像
    out = capsys.readouterr().out
    assert f"cloned account {source_account} -> {clone_id}" in out


def test_clone_requires_strategy_id(source_account, capsys):
    with pytest.raises(SystemExit):
        _run_create(["--create", "--clone-from", source_account])
    assert any("--strategy-id" in s for s in capsys.readouterr().err.splitlines())


def test_clone_missing_source_exits(source_account):
    with pytest.raises(SystemExit):
        _run_create(["--create", "--clone-from", "nonexist", "--strategy-id", "s1"])


def test_clone_overrides_apply(source_account):
    _run_create(["--create", "--clone-from", source_account, "--strategy-id", "sid-x",
                 "--name", "自定义名", "--account-id", "cln00001",
                 "--capital", "200000", "--start-date", "2026-08-01"])
    acc = db.get_sim_account("cln00001")
    assert acc["name"] == "自定义名" and acc["capital"] == 200000.0
    assert acc["start_date"] == "2026-08-01"
    st = db.read_sim_state("cln00001")
    assert st["positions"]                                     # state 仍克隆
```

注意：autostart 分支会调 `service.account_start`（spawn 子进程）。为离线化，在 `_run_create` 的 autostart 用例里 monkeypatch 更稳——但 `main()` 内部 import service。简化：给 `test_clone_*` 全部不加 `--autostart`（clone 逻辑与 start 无关，Task 3 手工 autostart）。上面第一个用例去掉 `--autostart` 参数。

修正后第一用例调用：`_run_create(["--create", "--clone-from", source_account, "--strategy-id", "wufu-v5.4-ding-report"])`。

另外 `scripts` 不是包：在测试顶部加

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
```

（若 repo 已有 conftest 处理则从简。）同时确认 `scripts/run_quant_sim.py` 可被 import（有 `if __name__ == "__main__"` 守卫 ✓）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_clone_account.py -q`
Expected: FAIL（unrecognized arguments: --clone-from）

- [ ] **Step 3: 实现 --clone-from**

`run_quant_sim.py` create 分支改为（完整替换 argparse 段与创建逻辑）：

```python
        p = argparse.ArgumentParser(prog="run_quant_sim.py --create")
        p.add_argument("--name", default=None)
        p.add_argument("--capital", type=float, default=None)
        p.add_argument("--stop-loss", dest="stop_loss", type=float, default=0.05)
        p.add_argument("--strategy-id", dest="strategy_id", required=True,
                       help="strategies 表已注册的策略 id")
        p.add_argument("--start-date", dest="start_date", default=None,
                       help="回放/补跑起始交易日（历史对齐用）")
        p.add_argument("--account-id", dest="account_id", default=None,
                       help="指定账户 id（验收对齐用固定 id）；缺省自动生成")
        p.add_argument("--clone-from", dest="clone_from", default=None,
                       help="克隆源账户：配置镜像 + sim_state 整行镜像续跑；需显式 --strategy-id")
        p.add_argument("--autostart", action="store_true",
                       help="创建后立即经内存门禁拉起子进程")
        a = p.parse_args(args[1:])
        import uuid
        from app.quant import db
        if a.clone_from:
            src = db.get_sim_account(a.clone_from)
            if not src:
                print(f"clone source account not found: {a.clone_from}", file=sys.stderr)
                sys.exit(1)
            name = a.name or f"{src['name']}-预报告"
            capital = a.capital if a.capital is not None else float(src["capital"])
            stop_loss = a.stop_loss if a.stop_loss != 0.05 or src["stop_loss"] == 0.05 \
                else float(src["stop_loss"])
            # ↑ stop-loss 无法区分"未传"与"传了默认0.05"，克隆场景直接以源值为准除非显式传非默认：
            if a.stop_loss is None:
                stop_loss = float(src["stop_loss"])
            start_date = a.start_date or src["start_date"]
            frequency = src.get("frequency") or "minute"
            aid = a.account_id or uuid.uuid4().hex[:8]
            db.insert_sim_account(aid, name, capital, stop_loss, "created",
                                  a.strategy_id, start_date, frequency)
            db.update_sim_account(aid, dingtalk_enabled=int(src.get("dingtalk_enabled") or 0))
            src_state = db.read_sim_state(a.clone_from)
            if src_state.get("dt"):
                db.upsert_sim_state(aid, src_state["cash"], src_state["positions_json"],
                                    src_state["net_value"], src_state["pnl"],
                                    src_state["start_cash"], src_state["stop_loss_log_json"],
                                    src_state["dt"])
            print(f"cloned account {a.clone_from} -> {aid}")
        else:
            if a.name is None or a.capital is None or a.start_date is None:
                print("--create 需要 --name/--capital/--start-date（或改用 --clone-from）",
                      file=sys.stderr)
                sys.exit(1)
            if a.account_id:
                # 固定 id：绕过 account_create 的 uuid 生成，直接落库
                db.insert_sim_account(a.account_id, a.name, float(a.capital),
                                      float(a.stop_loss), "created",
                                      a.strategy_id, a.start_date, "minute")
                aid = a.account_id
            else:
                from app.quant import service
                aid = service.account_create(a.name, a.capital, a.stop_loss,
                                             a.strategy_id, a.start_date)
            print(f"created account: {aid}")
        if a.autostart:
            from app.quant import service
            service.account_start(aid)
            print(f"started account: {aid}")
        return
```

清理要点：argparse 里 `--stop-loss` 改 `default=None` 并把上面那段别扭的三元删掉，统一：

```python
            stop_loss = float(src["stop_loss"]) if a.stop_loss is None else a.stop_loss
```

（即最终实现里 stop_loss 只允许这两行语义，勿保留示例中的过渡三元表达式。）

- [ ] **Step 4: 跑测试通过 + 回归**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_clone_account.py tests/quant/test_fix_sim.py -q`
Expected: 全 PASS。

- [ ] **Step 5: Commit**

```bash
git add backend/scripts/run_quant_sim.py backend/tests/quant/test_clone_account.py
git commit -m "feat: run_quant_sim.py 支持 --clone-from 镜像克隆模拟盘账户"
```

---

### Task 3: 注册策略 + 克隆账户 + 启动验收（主会话执行，触产库）

1. 注册策略行（save_strategy 固定 sid）：
   `cd backend && uv run python -c "from app.quant.strategies.store import save_strategy; save_strategy('wufu-v5.4-ding-report','五福v5.4钉钉版-预报告',open('data/quant_strategies/wufu-v5.4-ding-report.py').read())"` —— 注意相对路径，须在仓库根运行或改绝对路径。
2. 克隆+启动：`cd backend && uv run python scripts/run_quant_sim.py --create --clone-from d092ad90 --strategy-id wufu-v5.4-ding-report --autostart`
3. 验证：`ss -tlnp` 不适用（子进程非监听）；查 `sim_accounts.status='running'` 且 pid 存活；13:01 观察 sim_logs 出现预报告 notify、钉钉收到消息；13:10 对比实际成交。
