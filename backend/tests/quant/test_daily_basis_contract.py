"""日线落盘口径契约：``data/kline_daily`` 必须存**不复权原始价**。

背景（2026-10-22 事故）
-----------------------
``MootdxSource.get_daily`` 默认用 xdxr 把原始价换算成前复权，而
``_to_qfq`` 靠**帧内前一收盘价**推除权因子：

* 当日增量写（帧内无未来事件）→ 恰好等于原始价 ✓
* 事后批量回补（帧内有未来事件）→ 写成前复权 ✗

``mootdx_service.sync_daily`` 复用了默认 ``qfq=True``，于是 2023-04-19..
2023-12-20 共 165 个分区的历史日被写成前复权价；读侧
``DataSources.apply_qfq_daily`` 再乘 ``ex_factor`` → **重复复权**
（688032.SH 2023-05-04 偏 -58%，000001.SZ 偏 -18%）。

这些用例锁住三件事：
1. ``get_daily(qfq=False)`` 不换算、原样返回原始价；
2. 默认行为（``qfq=True``）不变 —— 回测桥仍拿前复权帧；
3. ``sync_daily`` 的取数调用**显式**传 ``qfq=False``（AST 静态守卫）。
"""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest

from app.quant.jqengine.datasource.mootdx_src import MootdxSource
from app.services import mootdx_service as ms


def _raw_frame() -> pd.DataFrame:
    ts = pd.Timestamp("2023-05-04 15:00:00")
    return pd.DataFrame(
        {"open": [556.8], "high": [560.0], "low": [550.0], "close": [556.8],
         "volume": [1000.0], "amount": [556800.0]},
        index=pd.DatetimeIndex([ts]),
    )


def _make_src(monkeypatch, calls: list[str]) -> MootdxSource:
    """不连网的 MootdxSource：取数直接返回固定原始价帧。"""
    src = MootdxSource()
    monkeypatch.setattr(src, "_with_server_retry", lambda fn, **kw: (_raw_frame(), None))
    monkeypatch.setattr(src, "_xdxr_rows", lambda sym: [{"category": 1}])

    def _fake_to_qfq(df, sym):
        calls.append(sym)
        out = df.copy()
        out["close"] = out["close"] * 0.42      # 任意"前复权"缩放，便于识别
        out.attrs["adj"] = "qfq"
        return out

    monkeypatch.setattr(src, "_to_qfq", _fake_to_qfq)
    return src


def test_get_daily_qfq_false_returns_raw(monkeypatch):
    calls: list[str] = []
    src = _make_src(monkeypatch, calls)
    df = src.get_daily("600000.SH", "20230504", "20230504", qfq=False)
    assert calls == [], "qfq=False 时不得调用 _to_qfq"
    assert df["close"].iloc[0] == pytest.approx(556.8), "必须原样返回原始价"
    assert df.attrs["adj"] == "raw"


def test_get_daily_default_still_qfq(monkeypatch):
    calls: list[str] = []
    src = _make_src(monkeypatch, calls)
    df = src.get_daily("600000.SH", "20230504", "20230504")
    assert calls, "默认（qfq=True）仍应换算，供回测桥使用"
    assert df["close"].iloc[0] == pytest.approx(556.8 * 0.42, rel=1e-9)
    assert df.attrs["adj"] == "qfq"


def test_guarded_get_daily_forwards_qfq_flag(monkeypatch):
    seen: list[bool] = []

    class _Spy:
        def get_daily(self, sym, start, end, *, qfq=True):
            seen.append(qfq)
            return _raw_frame()

    ms._guarded_get_daily(_Spy(), "600000.SH", "20230504", "20230504", qfq=False)
    ms._guarded_get_daily(_Spy(), "600000.SH", "20230504", "20230504")
    assert seen == [False, True]


def _calls_in(func: ast.FunctionDef, name: str) -> list[ast.Call]:
    out = []
    for node in ast.walk(func):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id == name:
            out.append(node)
    return out


def test_sync_daily_fetches_raw_for_storage():
    """静态守卫：sync_daily 的日线取数必须显式 qfq=False（落盘存原始价）。"""
    path = Path(ms.__file__).resolve()
    tree = ast.parse(path.read_text(encoding="utf-8"))
    funcs = [n for n in ast.walk(tree)
             if isinstance(n, ast.FunctionDef) and n.name == "sync_daily"]
    assert funcs, "mootdx_service.sync_daily 不存在"
    calls = _calls_in(funcs[0], "_guarded_get_daily")
    assert calls, "sync_daily 未见 _guarded_get_daily 调用（结构变了？请更新守卫）"
    flags = []
    for call in calls:
        kw = {k.arg: k.value for k in call.keywords if k.arg}
        flags.append(isinstance(kw.get("qfq"), ast.Constant) and kw["qfq"].value is False)
    assert all(flags), (
        "sync_daily 必须用 _guarded_get_daily(..., qfq=False) 落盘原始价；"
        "否则回补的历史日会被写成前复权价，读侧 apply_qfq_daily 重复复权"
    )
