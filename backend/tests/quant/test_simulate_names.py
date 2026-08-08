"""模拟盘进程名称解析模块测试。"""
from __future__ import annotations

from app.quant.simulate import names


def test_resolve_name_uses_client_map(monkeypatch):
    calls = []

    def _fake_get_stock_names(self, codes=None):
        calls.append(codes)
        return {"159985": "豆粕ETF华夏", "600000": "浦发银行"}

    monkeypatch.setattr(
        "app.quant.datasource.network_client.StockDataClient.get_stock_names",
        _fake_get_stock_names,
    )
    # 清模块级缓存，强制重建
    names._NAMES = None
    try:
        nm = names.get_name_map()
        assert nm.get("159985") == "豆粕ETF华夏"
        assert nm.get("600000") == "浦发银行"
        # 解析：JQ 码命中（按纯代码查）
        assert names.resolve_name("159985.XSHE") == "豆粕ETF华夏"
        # 未命中回退代码
        assert names.resolve_name("999999.XSHG") == "999999.XSHG"
        # 进程内缓存：第二次不再调 client
        names.get_name_map()
        assert len(calls) == 1
    finally:
        names._NAMES = None


def test_get_name_map_empty_on_error(monkeypatch):
    def _boom(self, codes=None):
        raise RuntimeError("service down")

    monkeypatch.setattr(
        "app.quant.datasource.network_client.StockDataClient.get_stock_names",
        _boom,
    )
    # 聚宽快照也失败 → 空映射
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.jq_names.load_jq_names",
        lambda: (_ for _ in ()).throw(RuntimeError("no snapshot")),
    )
    names._NAMES = None
    try:
        assert names.get_name_map() == {}
        assert names.resolve_name("600000.XSHG") == "600000.XSHG"
    finally:
        names._NAMES = None


def test_resolve_name_none_map_fallback(monkeypatch):
    """名称映射为空时回退代码本身（不依赖真实服务）。"""
    monkeypatch.setattr(names, "get_name_map", lambda: {})
    assert names.resolve_name("159985.XSHE") == "159985.XSHE"


def test_get_name_map_falls_back_to_jq_snapshot(monkeypatch):
    """通达信名缺失的标的（如 LOF 501018）回退聚宽快照名。"""
    monkeypatch.setattr(
        "app.quant.datasource.network_client.StockDataClient.get_stock_names",
        lambda self, codes=None: {"159985": "豆粕ETF华夏"},
    )
    monkeypatch.setattr(
        "app.quant.jqengine.engine.jq.jq_names.load_jq_names",
        lambda: {"501018.XSHG": "南方原油"},
    )
    names._NAMES = None
    try:
        nm = names.get_name_map()
        # 通达信名优先
        assert nm.get("159985") == "豆粕ETF华夏"
        # 缺失时补聚宽快照名（JQ码转纯代码键）
        assert nm.get("501018") == "南方原油"
        assert names.resolve_name("501018.XSHG") == "南方原油"
    finally:
        names._NAMES = None
