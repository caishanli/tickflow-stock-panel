"""1 Core 铁律的机器执法（见 docs/quant-core-contract.md §0）。

一个语义只许有一个实现，正统位置 ``app/quant/core/``。引擎文件只许做
翻译（码制/签名/state 形状）+ 薄适配（一行转调、无运算分支）；新增同名
语义定义、或委托链断裂，即失败。
"""
from __future__ import annotations

import ast
from pathlib import Path

Q = Path(__file__).resolve().parents[2] / "app" / "quant"
CORE = "core/"


def _defs(name):
    """返回 {relpath: [lineno]}，值为所有 `def <name>` 定义点。"""
    out = {}
    for p in sorted(Q.rglob("*.py")):
        try:
            tree = ast.parse(p.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        hits = [n.lineno for n in ast.walk(tree)
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
        if hits:
            out[str(p.relative_to(Q))] = hits
    return out


def _func_node(path, name):
    tree = ast.parse((Q / path).read_text(encoding="utf-8"))
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return n
    raise AssertionError(f"{path} 缺少 def {name}")


def _assert_thin_adapter(path, name):
    """薄适配校验：docstring + 至多一条赋值 + 一行 return，无运算/分支。"""
    node = _func_node(path, name)
    body = [n for n in node.body if not isinstance(n, ast.Expr)]
    assert body and isinstance(body[-1], ast.Return), \
        f"{path}:{name} 不再是薄转调（末尾不是 return）"
    assert len(body) <= 2, f"{path}:{name} 超过一行转调 ({len(body)} 句)"
    banned = (ast.BinOp, ast.Compare, ast.If, ast.For, ast.While, ast.BoolOp)
    assert not any(isinstance(n, banned) for n in ast.walk(node)), \
        f"{path}:{name} 适配体内出现运算/分支，违反 1 Core §0.2"


def _assert_delegates(path, caller, callee):
    """委托链校验：caller 函数体内必须调用 callee。"""
    node = _func_node(path, caller)
    calls = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            f = n.func
            if isinstance(f, ast.Name):
                calls.add(f.id)
            elif isinstance(f, ast.Attribute):
                calls.add(f.attr)
    assert callee in calls, f"{path}:{caller} 未调用 {callee}，委托链断裂"


# ---- 唯一实现：正统定义点只许在 core/，别处零定义 ----

def test_tick_canonical_in_core():
    assert set(_defs("round_to_tick")) == {CORE + "tick.py"}, _defs("round_to_tick")
    assert set(_defs("tick_size")) == {CORE + "tick.py"}, _defs("tick_size")


def test_is_etf_converged():
    """税种判定零本地定义（2026-09 收敛完成；_is_etf_jq_code 系宇宙过滤，另名保留）。"""
    assert _defs("_is_etf") == {}, _defs("_is_etf")
    assert set(_defs("is_etf")) == {CORE + "instruments.py"}, _defs("is_etf")


def test_classify_fund_converged():
    assert _defs("_fund_instrument_type") == {}, _defs("_fund_instrument_type")
    assert set(_defs("classify_fund")) == {CORE + "instruments.py"}


def test_limit_prices_converged():
    assert _defs("_limit_prices_from_prev_close") == {}, \
        _defs("_limit_prices_from_prev_close")


def test_revoke_single_definition():
    assert set(_defs("revoke_future_split_factor")) == \
        {"jqengine/datasource/manager.py"}, _defs("revoke_future_split_factor")


def test_mem_daily_usable_single_real_definition():
    sites = _defs("mem_daily_usable")
    assert "jqengine/datasource/manager.py" in sites, sites
    extra = set(sites) - {"jqengine/datasource/manager.py"}
    assert extra <= {"jqengine/engine/jq/api.py"}, f"新增实现：{sites}"
    for path in extra:
        _assert_thin_adapter(path, "mem_daily_usable")


def test_stamp_tax_no_local_definition():
    """DEFAULT_STAMP_TAX 不许赋值定义，只许从 core import 别名。"""
    for rel in ("jqengine/engine/jq/api.py", "simulate/matcher.py",
                "ptradeengine/ptrade_api.py"):
        tree = ast.parse((Q / rel).read_text(encoding="utf-8"))
        assigns = [n for n in tree.body if isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "DEFAULT_STAMP_TAX" for t in n.targets)]
        assert not assigns, f"{rel} 仍本地定义税率"
        imported = [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
                    and (n.module or "") in ("core", "core.instruments")
                    and any(a.asname == "DEFAULT_STAMP_TAX" for a in n.names)]
        assert imported, f"{rel} 的 DEFAULT_STAMP_TAX 未从 core import"


# ---- 薄适配允许表（只减不增）：码制归一化 + 本地名源，无公式 ----

_LIMIT_RATE_ALLOW = {
    "jqcompat.py",
    "ptradecompat.py",
    "ptradeengine/ptrade_api.py",
}


def test_limit_rate_adapters_only():
    # 正统（无下划线）唯一在 core；引擎侧薄适配（带下划线）只许允许表
    assert set(_defs("limit_rate")) == {CORE + "limits.py"}, _defs("limit_rate")
    sites = set(_defs("_limit_rate"))
    assert sites <= _LIMIT_RATE_ALLOW, \
        f"新增 _limit_rate 实现：{sites - _LIMIT_RATE_ALLOW}"
    for path in sites:
        _assert_thin_adapter(path, "_limit_rate")


def test_core_math_single_definition():
    """core 数学函数零引擎侧重复定义（同名出现即有人另起炉灶）。"""
    for name, canonical in (
        ("fill_price", CORE + "fees.py"),
        ("resolve_commission", CORE + "fees.py"),
        ("commission", CORE + "fees.py"),
        ("stamp_tax", CORE + "fees.py"),
        ("stamp_tax_rate", CORE + "fees.py"),
        ("round_buy_lot", CORE + "lots.py"),
        ("affordable_shares", CORE + "lots.py"),
        ("resolve_live_price", CORE + "pricing.py"),
        ("limit_prices_from_prev_close", CORE + "limits.py"),
        ("order_value_amount", CORE + "execution.py"),
        ("target_percent_amount", CORE + "execution.py"),
    ):
        assert set(_defs(name)) == {canonical}, f"{name}: {_defs(name)}"


# ---- 委托链：两 order() 必须调 execute_order ----

def test_orders_delegate_to_core():
    _assert_delegates("jqengine/engine/jq/api.py", "order", "execute_order")
    _assert_delegates("ptradeengine/ptrade_api.py", "order", "execute_order")
    assert set(_defs("execute_order")) == {CORE + "execution.py"}, \
        _defs("execute_order")
