"""1 Core 铁律的机器执法（见 docs/quant-core-contract.md §0）。

一个语义只许有一个实现。允许表（ALLOWLIST）只减不增：
收敛掉一处重复，就从表里删一处；新增同名语义定义即失败。
"""
from __future__ import annotations

import ast
from pathlib import Path

Q = Path(__file__).resolve().parents[2] / "app" / "quant"


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


# ---- 唯一实现：这些语义只许一处定义，别处只能 import ----

def test_round_to_tick_single_definition():
    assert _defs("round_to_tick") == {"tick.py": _defs("round_to_tick")["tick.py"]}, \
        f"round_to_tick 出现第二实现：{_defs('round_to_tick')}"


def test_tick_size_single_definition():
    assert set(_defs("tick_size")) == {"tick.py"}, _defs("tick_size")


def test_revoke_future_split_factor_single_definition():
    assert set(_defs("revoke_future_split_factor")) == \
        {"jqengine/datasource/manager.py"}, _defs("revoke_future_split_factor")


def test_mem_daily_usable_single_real_definition():
    """唯一实现 DataManager.mem_daily_usable；jq api 的同名函数只许是薄转调。"""
    sites = _defs("mem_daily_usable")
    assert "jqengine/datasource/manager.py" in sites, sites
    extra = set(sites) - {"jqengine/datasource/manager.py"}
    assert extra <= {"jqengine/engine/jq/api.py"}, f"新增实现：{sites}"
    if extra:
        node = _func_node("jqengine/engine/jq/api.py", "mem_daily_usable")
        # 薄包装：docstring + 一行 return，不许算术/比较/分支（§0 第2条）
        body = [n for n in node.body if not isinstance(n, ast.Expr)]
        assert len(body) == 1 and isinstance(body[0], ast.Return), \
            "api._mem_daily_usable 不再是薄转调，有人往里加了逻辑"
        banned = (ast.BinOp, ast.Compare, ast.If, ast.For, ast.While, ast.BoolOp)
        assert not any(isinstance(n, banned) for n in ast.walk(node)), \
            "api._mem_daily_usable 包装体内出现运算/分支，违反 1 Core §0.2"


# ---- 收敛中：允许表只减不增 ----

_IS_ETF_ALLOW = {
    "jqengine/engine/jq/api.py",
    "simulate/matcher.py",
    "ptradeengine/ptrade_api.py",
}


def test_is_etf_no_new_definition():
    """税种判定 _is_etf 允许表：现存三处，禁止第四处；合一时逐处删表。"""
    sites = set(_defs("_is_etf"))
    assert sites <= _IS_ETF_ALLOW, f"新增 _is_etf 实现：{sites - _IS_ETF_ALLOW}"
    assert sites == _IS_ETF_ALLOW, \
        f"允许表过期（已收敛？请删表）：缺 { _IS_ETF_ALLOW - sites}"


def test_stamp_tax_all_env_driven():
    """所有 DEFAULT_STAMP_TAX 定义必须读 QUANT_SIM_STAMP_TAX，禁止硬编码税率。"""
    for rel in ("jqengine/engine/jq/api.py", "simulate/matcher.py",
                "ptradeengine/ptrade_api.py"):
        src = (Q / rel).read_text(encoding="utf-8")
        node = None
        tree = ast.parse(src)
        for n in tree.body:
            if isinstance(n, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "DEFAULT_STAMP_TAX" for t in n.targets):
                node = n
        assert node is not None, f"{rel} 缺少 DEFAULT_STAMP_TAX"
        assert "QUANT_SIM_STAMP_TAX" in ast.dump(node.value), \
            f"{rel} 的 DEFAULT_STAMP_TAX 未读 env，硬编码税率违反 1 Core"
