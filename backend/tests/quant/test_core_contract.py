"""量化执行核心语义契约的机器可执行部分（见 docs/quant-core-contract.md）。

- 分类一致性：宇宙全量逐码校验 tick/税种/取整三处口径（新前缀自动落网）。
- 费率公式：佣金取max/印花税单边ETF免（经 Matcher，与引擎同式）。
- 禁静默 stub：AST 扫描兼容层，函数体只剩 pass/return None 即失败（允许表除外）。
- Face 费用路径：各方言的 set_* 必须真实记录（曾全是 no-op）。
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SNAPSHOT = _REPO / "data" / "quant_kline" / "etf_universe_snapshot.json"


def _snapshot_codes():
    import json

    snap = json.loads(_SNAPSHOT.read_text(encoding="utf-8"))
    return list(snap.get("codes") or [])


@pytest.mark.skipif(not _SNAPSHOT.exists(), reason="需要仓库 data 快照")
def test_taxonomy_consistent_with_universe():
    """契约 §3：宇宙每个码在三处分类必须一致（ETF/LOF→tick 0.001+免税）。

    新品种前缀一旦出现，若某处漏判即失败，强制显式分类（52 系教训）。
    无 rqalpha 环境跳过（jqcompat 顶层 import rqalpha）。
    """
    pytest.importorskip("rqalpha")
    from app.quant import jqcompat
    from app.quant.simulate.matcher import _is_etf as sim_is_etf
    from app.quant.tick import tick_size

    codes = _snapshot_codes()
    assert len(codes) > 1000, "宇宙快照异常缩小，检查数据源"
    bad = []
    for code in codes:
        t = jqcompat._fund_instrument_type(code)
        tick = tick_size(code)
        exempt = sim_is_etf(code)
        if t in ("ETF", "LOF"):
            ok = (tick == 0.001 and exempt)
        elif t == "CS":
            ok = (tick == 0.01 and not exempt)
        else:  # pragma: no cover - 未知类型必须显式处理
            ok = False
        if not ok:
            bad.append((code, t, tick, exempt))
    assert not bad, f"分类不一致的前 10 个: {bad[:10]}（共 {len(bad)}）"


def _matcher_cash_after_sell(code, price, amount, avg_cost, fee, stamp_tax, slippage):
    from app.quant.simulate.matcher import Matcher

    m = Matcher(0.03)
    state = {"cash": 0.0,
             "positions": {code: {"amount": float(amount), "avg_cost": avg_cost,
                                  "price": avg_cost}},
             "stop_loss_log": [], "dt": "2026-07-17 10:00:00"}
    out = m.step(state, {code: price}, fee=fee, stamp_tax=stamp_tax, slippage=slippage)
    return out["cash"]


def test_fee_commission_floor_and_rate():
    """契约 §2：佣金 = max(成交额×费率, 最低)，双边同式。"""
    # 大额：按费率（600000.XSHG 股票 tick 0.01：9.0×0.999=8.991→8.99）
    cash = _matcher_cash_after_sell("600000.XSHG", 9.0, 5000.0, 10.0,
                                    fee=0.0003, stamp_tax=0.0, slippage=0.001)
    assert cash == pytest.approx(5000.0 * 8.99 * (1 - 0.0003), abs=1e-2)
    # 小额：触最低佣金下限（显式 min_commission=5；引擎默认 min 为 0）
    cash = _matcher_cash_after_sell("600000.XSHG", 9.0, 100.0, 10.0,
                                    fee=0.0003, stamp_tax=0.0, slippage=0.0)
    # 先确认默认 min=0 的行为（900×0.0003=0.27 <5 仍收 0.27），再显式测下限
    assert cash == pytest.approx(100.0 * 9.0 - 0.27, abs=1e-2)
    from app.quant.simulate.matcher import Matcher

    m = Matcher(0.03)
    out = m.step({"cash": 0.0, "positions": {
        "600000.XSHG": {"amount": 100.0, "avg_cost": 10.0, "price": 10.0}},
        "stop_loss_log": [], "dt": "2026-07-17 10:00:00"},
        {"600000.XSHG": 9.0}, fee=0.0003, stamp_tax=0.0, slippage=0.0,
        min_commission=5.0)
    assert out["cash"] == pytest.approx(100.0 * 9.0 - 5.0, abs=1e-2)


def test_fee_stamp_only_sell_and_etf_exempt():
    """契约 §2：印花税仅卖出、股票 0.05%、ETF 全免（买入不收股票印花税另见引擎）。"""
    from app.quant.simulate.matcher import Matcher

    m = Matcher(0.03)
    # ETF 卖出：佣金照收，印花税为 0（510300 tick 0.001：9.0×0.999=8.991）
    out = m.step({"cash": 0.0, "positions": {
        "510300.XSHG": {"amount": 5000.0, "avg_cost": 10.0, "price": 10.0}},
        "stop_loss_log": [], "dt": "2026-07-17 10:00:00"},
        {"510300.XSHG": 9.0}, fee=0.0003, stamp_tax=0.0005, slippage=0.001)
    assert out["cash"] == pytest.approx(5000.0 * 8.991 * (1 - 0.0003), abs=1e-2)
    # 股票卖出：佣金 + 印花税双扣
    out = m.step({"cash": 0.0, "positions": {
        "600000.XSHG": {"amount": 5000.0, "avg_cost": 10.0, "price": 10.0}},
        "stop_loss_log": [], "dt": "2026-07-17 10:00:00"},
        {"600000.XSHG": 9.0}, fee=0.0003, stamp_tax=0.0005, slippage=0.001)
    assert out["cash"] == pytest.approx(5000.0 * 8.99 * (1 - 0.0003 - 0.0005), abs=1e-2)


def test_order_target_percent_face_parity():
    """契约：order_target_percent 两边引擎都要有（回测侧曾缺失，调了 NameError）。

    语义对等由逐笔对比验收（dual_v54_ptrade 矩阵），此处只锁存在性。
    """
    from app.quant.ptradeengine import ptrade_api as ptapi

    assert callable(ptapi.order_target_percent)
    rqalpha = pytest.importorskip("rqalpha")
    assert rqalpha is not None
    from app.quant import ptradecompat

    assert callable(ptradecompat.order_target_percent)


def _stubbed_public_funcs(path: Path):
    """顶层 public 函数中函数体只剩 pass / return None / ... 的（docstring 除外）。"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith("_"):
            continue
        body = [n for n in node.body
                if not (isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant))]
        if len(body) != 1:
            continue
        only = body[0]
        if isinstance(only, ast.Pass):
            found.append(f"{node.name}:pass")
        elif isinstance(only, ast.Expr) and isinstance(only.value, ast.Constant) \
                and only.value.value is Ellipsis:
            found.append(f"{node.name}:...")
        elif isinstance(only, ast.Return) and (
                only.value is None or (isinstance(only.value, ast.Constant)
                                       and only.value.value is None)):
            found.append(f"{node.name}:returnNone")
    return found


# 有意保留的空实现（无对应引擎语义，显式声明而非遗漏）
STUB_ALLOWLIST = {
    # 期权接口：现货 core 无对应语义，调用应显式失败而非近似
    "jqcompat.set_option",
    # jq 默认空钩子：用户策略覆盖，空实现即正确语义
    "jqapi.init",
    # 聚宽 record（画时间序列用）：rqalpha 6.x 原生无此 API，净值/成交由
    # analyser 回收；保留 shim 仅防策略 NameError（jqcompat 注释已说明）
    "jqcompat.record",
}


def test_no_silent_stubs_in_compat_layers():
    """契约 §7.1：兼容层禁止静默 stub（曾致费用/滑点整段分叉且零报错）。"""
    mods = {
        "jqcompat": _REPO / "backend" / "app" / "quant" / "jqcompat.py",
        "ptradecompat": _REPO / "backend" / "app" / "quant" / "ptradecompat.py",
        "jqapi": _REPO / "backend" / "app" / "quant" / "jqengine" / "engine" / "jq" / "api.py",
        "ptapi": _REPO / "backend" / "app" / "quant" / "ptradeengine" / "ptrade_api.py",
    }
    violations = []
    for mod, path in mods.items():
        for stub in _stubbed_public_funcs(path):
            if f"{mod}.{stub.split(':')[0]}" not in STUB_ALLOWLIST:
                violations.append(f"{mod}.{stub}")
    assert not violations, f"静默 stub（实现或入允许表并注明理由）: {violations}"


@pytest.mark.parametrize("modname", ["ptradecompat"])
def test_ptrade_cost_path_records(modname):
    """契约 §2：ptrade 回测侧费用必须真实记录（曾 no-op 致回测/补跑分叉）。"""
    pytest.importorskip("rqalpha")  # 委托给 jqcompat（顶层 import rqalpha）
    import importlib

    mod = importlib.import_module(f"app.quant.{modname}")
    mod.set_commission(commission_ratio=0.0001, min_commission=5)
    assert mod._LAST_COMMISSION == {"commission_ratio": 0.0001, "min_commission": 5.0}
    from app.quant import jqcompat

    assert jqcompat._ORDER_COST_OVERRIDE is not None
    assert jqcompat._ORDER_COST_OVERRIDE["close_commission"] == 0.0001
    mod.set_slippage(0.0002)
    assert mod._LAST_SLIPPAGE == pytest.approx(0.0002)
    assert jqcompat._SLIPPAGE_OVERRIDE == pytest.approx(0.0002)


def _pt_fresh_api():
    """ptrade 引擎最小 harness（镜像 test_ptradeengine._fresh_api）。"""
    from app.quant.ptradeengine import ptrade_api as api

    class _StubDm:
        def get_minute_price_at(self, code, dt):
            return None

        def fetch(self, *a, **k):
            import pandas as pd
            return pd.DataFrame()

    api._reset(_StubDm(), 0.0001, 0.0001, 100000.0)
    api._state["minute_mode"] = True
    return api


def test_ptapi_order_tick_matches_jq():
    """移植验证：ptrade engine 撮合取整与 jq 同口径（曾裸 round(fill,3)，
    股票少取一位）。"""
    from app.quant.ptradeengine.context import PtradePosition

    api = _pt_fresh_api()
    try:
        api._state["minute_prices"] = {"600000.SS": 9.005}
        assert api.order("600000.SS", 100) is True
        # 买入 9.005×1.0001=9.0059 → 股票 tick 取整 9.01（旧 round(...,3)=9.006）
        assert api._state["trades"][-1]["price"] == pytest.approx(9.01)
        # 卖出需可卖持仓（T+1）：手工建仓
        pos = PtradePosition()
        pos.amount = 5000.0
        pos.today_amount = 0.0  # closeable_amount 由此派生（amount - today_amount）
        pos.avg_cost = 10.0
        pos.price = 9.0
        api._state["ctx"].portfolio.positions["600000.SS"] = pos
        api._state["minute_prices"] = {"600000.SS": 9.0}
        assert api.order("600000.SS", -100) is True
        # 卖出 9.0×0.9999=8.9991 → 股票 tick 取整 9.0（旧 round(...,3)=8.999）
        assert api._state["trades"][-1]["price"] == pytest.approx(9.0)
        api._state["minute_prices"] = {"510300.SS": 9.0}
        assert api.order("510300.SS", 100) is True
        # ETF 买入 9.0×1.0001=9.0009 → 9.001（tick 0.001；此处与旧值一致，仅防回退）
        assert api._state["trades"][-1]["price"] == pytest.approx(9.001)
        assert api._state["trades"][-1]["fee"] == pytest.approx(0.09)
        assert api._state["trades"][-1]["tax"] == 0.0  # ETF 免印花税
    finally:
        api._state["manager"] = None


def test_ptapi_is_etf_covers_18():
    """移植验证：ptrade 税种判定补 18 前缀（与 jqengine/matcher 一致）。"""
    from app.quant.ptradeengine.ptrade_api import _is_etf

    assert _is_etf("180012.XSHE") is True
    assert _is_etf("159876.XSHE") is True
    assert _is_etf("520830.XSHG") is True
    assert _is_etf("600000.XSHG") is False


def test_ptapi_batch_guard_refetch():
    """移植验证：ptrade get_history 批量窄帧走逐只回源（与 jq 同规则）。"""
    import pandas as pd

    from app.quant.ptradeengine import ptrade_api as api

    idx = pd.DatetimeIndex([f"2026-05-{d:02d}" for d in range(6, 30)] +
                             [f"2026-06-{d:02d}" for d in range(1, 24)])
    idx = idx[idx.weekday < 5]
    full = pd.DataFrame({"close": [1.0] * len(idx), "money": [1e7] * len(idx)},
                        index=idx)
    narrow = full.iloc[-8:]

    class _FakeMgr:
        def __init__(self):
            self._daily_mem = {}
            self.fetch_calls = []

        def fetch(self, method, sec, start=None, end=None):
            self.fetch_calls.append(sec)
            self._daily_mem[f"get_daily_{sec}"] = full
            return full

    mgr = _FakeMgr()
    api._reset(mgr, 0.0001, 0.0001, 100000.0)
    try:
        mgr._daily_mem["get_daily_510300.XSHG"] = narrow
        out = api.get_history(25, "1d", "close",
                              security_list=["510300.SS"])
        assert mgr.fetch_calls, "窄帧必须回源"
        assert len(out) == 25
    finally:
        api._state["manager"] = None
