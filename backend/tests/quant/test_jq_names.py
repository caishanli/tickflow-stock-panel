"""模拟盘策略侧名称源：聚宽名/通达信名可切换。"""
from __future__ import annotations

from typing import ClassVar

from app.quant.jqengine.engine.jq import jq_names


def test_load_jq_names_reads_snapshot(tmp_path, monkeypatch):
    """jq_names.load_jq_names 读 etf_universe_snapshot.json。"""
    import datetime as _dt
    import json
    snap = tmp_path / "etf_universe_snapshot.json"
    snap.write_text(json.dumps({
        "fetched_at": (_dt.datetime.now() - _dt.timedelta(days=1)).isoformat(),
        "codes": ["511880.XSHG"],
        "names": {"511880.XSHG": "货币ETF-A"},
        "list_dates": {},
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(jq_names, "SNAPSHOT_PATH", str(snap))
    jq_names._CACHE = None
    try:
        names = jq_names.load_jq_names()
        assert names.get("511880.XSHG") == "货币ETF-A"
    finally:
        jq_names._CACHE = None


def test_get_all_securities_uses_jq_names_when_enabled(monkeypatch):
    """开关为 jq 时 get_all_securities 返回聚宽名。"""
    from app.quant.jqengine.engine.jq import api

    monkeypatch.setattr(api, "_state", {"manager": _FakeMgr()})
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._name_source",
        lambda: "jq",
    )
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._jq_names",
        lambda: {"511880.XSHG": "货币ETF-A"},
    )
    df = api.get_all_securities(["etf"])
    row = df.loc["511880.XSHG"]
    assert row["display_name"] == "货币ETF-A"


def test_get_security_info_display_name_uses_real_name(monkeypatch):
    """get_security_info().display_name 返回真实名称（而非代码）。

    策略 get_security_name 兜底依赖该字段；走弱期 etf_names_dict 为空时，
    若这里返回代码会导致买卖通知显示成「代码(代码)」。
    """
    from app.quant.jqengine.engine.jq import api

    monkeypatch.setattr(
        api, "_state",
        {"manager": _FakeMgr(), "sec_names": {}})
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._name_source",
        lambda: "jq",
    )
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._jq_names",
        lambda: {"501018.XSHG": "南方原油", "159985.XSHE": "豆粕ETF华夏"},
    )
    assert api.get_security_info("501018.XSHG").display_name == "南方原油"
    assert api.get_security_info("159985.XSHE").display_name == "豆粕ETF华夏"


class _FakeMgr:
    sources: ClassVar[dict] = {"network": object()}

    def fetch(self, method, *a, **k):
        return ["511880.XSHG"]
