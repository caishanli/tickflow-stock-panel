"""ETF 名称聚宽快照兜底测试。

背景：501018（南方原油，QDII/LOF 类）在 kirin_sim 交易记录与持仓里
显示为代码。根因：TickFlow 免费 ETF 名单缺这类 57 只 → stockdata 名称
映射没有它；本地 instruments_etf 缺失；策略引擎 service 模式不查快照。
聚宽快照明明有这个名字，三条链都没用上它。
"""
from __future__ import annotations

from typing import ClassVar

import polars as pl


def _write_stock_instruments(root, rows: list[tuple[str, str]]) -> None:
    d = root / "instruments"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": [s for s, _ in rows],
                  "name": [n for _, n in rows]}).write_parquet(
        d / "instruments.parquet")


def test_stockdata_name_map_merges_jq_snapshot(tmp_path, monkeypatch):
    """免费 ETF 名单缺 501018 时，快照兜底补上（setdefault，不覆盖已有名）。"""
    import app.quant.jqengine.engine.jq.jq_names as jq_names_mod
    from app.services.stockdata.sources import DataSources

    _write_stock_instruments(tmp_path, [("600000.SH", "浦发银行")])
    monkeypatch.setattr(
        "app.services.index_sync._fetch_instruments_by_type",
        lambda *a, **k: pl.DataFrame(
            {"symbol": [], "name": [], "code": []}).cast(
            {"symbol": pl.String, "name": pl.String, "code": pl.String}),
    )
    monkeypatch.setattr(jq_names_mod, "load_jq_names",
                        lambda: {"501018.XSHG": "南方原油",
                                 "600000.SH": "快照旧名-不得覆盖"})

    src = DataSources(data_root=str(tmp_path), mootdx_factory=None,
                      fetch_workers=2)
    m = src.get_stock_names()
    assert m.get("501018") == "南方原油"
    assert m.get("600000") == "浦发银行"


def test_engine_get_security_name_falls_back_to_jq_snapshot(monkeypatch):
    """service 模式 service 名单与 etf_list 都缺 501018 时，回退快照名。"""
    from app.quant.jqengine.engine.jq import api

    class _Net:
        def get_stock_names(self):
            return {}

    class _Mgr:
        sources: ClassVar[dict] = {"network": _Net()}

        def fetch(self, method, *a, **k):
            return []

    monkeypatch.setattr(api, "_state", {"manager": _Mgr()})
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._name_source", lambda: "service")
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._jq_names",
        lambda: {"501018.XSHG": "南方原油"})
    assert api.get_security_name("501018.XSHG") == "南方原油"
    # 完全未知代码仍回退代码本身
    assert api.get_security_name("999999.XSHG") == "999999.XSHG"


def test_stockdata_name_map_merges_snapshot_on_cache_hit(tmp_path, monkeypatch):
    """陈旧缓存文件缺 501018 时，加载后同样用快照补（不钉死缺口）。"""
    import json

    import app.quant.jqengine.engine.jq.jq_names as jq_names_mod
    from app.services.stockdata.sources import DataSources

    cache = tmp_path / ".stock_names_cache.json"
    cache.write_text(json.dumps({"600000": "浦发银行"}), encoding="utf-8")
    monkeypatch.setattr(jq_names_mod, "load_jq_names",
                        lambda: {"501018.XSHG": "南方原油"})

    src = DataSources(data_root=str(tmp_path), mootdx_factory=None,
                      fetch_workers=2)
    m = src.get_stock_names()
    assert m.get("501018") == "南方原油"
    assert m.get("600000") == "浦发银行"


def test_engine_get_security_name_smart_prefix_stable_across_calls(monkeypatch):
    """smart 模式快照兜底：连续两次调用结果一致（缓存须写回带前缀值）。"""
    from app.quant.jqengine.engine.jq import api

    class _Net:
        def get_stock_names(self):
            return {}

    class _Mgr:
        sources: ClassVar[dict] = {"network": _Net()}

        def fetch(self, method, *a, **k):
            return []

    monkeypatch.setattr(api, "_state", {"manager": _Mgr()})
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._name_source", lambda: "smart")
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.api._jq_names",
        lambda: {"501018.XSHG": "南方原油"})
    monkeypatch.setattr(
        "app.quant.smart_classification.get_smart_name",
        lambda code, base: f"[TEST] {base}")
    first = api.get_security_name("501018.XSHG")
    second = api.get_security_name("501018.XSHG")
    assert first == "[TEST] 南方原油"
    assert second == first


def test_relative_data_dir_anchored_to_repo_root(tmp_path, monkeypatch):
    """DATA_DIR=./data（.env 现状）不得随进程 CWD 漂移到 backend/data。

    根因：服务进程 CWD=backend/ 时相对路径解析到 backend/data 影子目录，
    jq 快照/回测快照/引擎磁盘缓存全部错位且静默失败。
    """
    import os

    from app.quant.jqengine import config as qc

    monkeypatch.setenv("DATA_DIR", "./data")
    monkeypatch.chdir(tmp_path)  # 任意 CWD 下都必须锚定仓库根
    cfg = qc.load_config()
    assert os.path.isabs(cfg["DATA_DIR"])
    assert cfg["DATA_DIR"] == os.path.normpath(
        os.path.join(os.path.dirname(qc.BASE_DIR), "data"))


def test_snapshot_falls_back_to_quant_kline_subdir(tmp_path, monkeypatch):
    """快照实际落在 quant_kline/（默认口径）时，扁平 DATA_DIR 也能读到。

    根因：默认口径嵌套 quant_kline，.env/容器口径扁平；双路径兼容两种布局。
    """
    import datetime as _dt
    import json

    from app.quant.jqengine.engine.jq import jq_names

    sub = tmp_path / "quant_kline"
    sub.mkdir()
    (sub / "etf_universe_snapshot.json").write_text(json.dumps({
        "fetched_at": (_dt.datetime.now() - _dt.timedelta(days=1)).isoformat(),
        "codes": ["501018.XSHG"],
        "names": {"501018.XSHG": "南方原油"},
        "list_dates": {},
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(jq_names, "SNAPSHOT_PATH",
                        str(tmp_path / "etf_universe_snapshot.json"))
    monkeypatch.setitem(jq_names._JQ_CONFIG, "DATA_DIR", str(tmp_path))
    jq_names._CACHE = None
    try:
        assert jq_names.load_jq_names().get("501018.XSHG") == "南方原油"
    finally:
        jq_names._CACHE = None
