"""最终评审 fixes 测试：回测移除二次清洗、jq_names 快照过期、缓存钉住防护。"""
from __future__ import annotations

import json
import datetime as _dt

import pytest

from app.quant import db
from app.quant.jqengine.engine.jq import jq_names


def test_jq_names_skips_stale_snapshot(tmp_path, monkeypatch):
    """快照过期（>30天）时 jq_names 返回空（回退 tdx）。"""
    old = (_dt.datetime.now() - _dt.timedelta(days=31)).isoformat()
    snap = tmp_path / "etf_universe_snapshot.json"
    snap.write_text(json.dumps({
        "fetched_at": old,
        "codes": ["511880.XSHG"],
        "names": {"511880.XSHG": "货币ETF-A"},
        "list_dates": {},
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(jq_names, "SNAPSHOT_PATH", str(snap))
    monkeypatch.setattr(jq_names, "MAX_AGE", _dt.timedelta(days=30))
    jq_names._CACHE = None
    try:
        assert jq_names.load_jq_names() == {}
    finally:
        jq_names._CACHE = None


def test_jq_names_uses_fresh_snapshot(tmp_path, monkeypatch):
    """快照新鲜时返回聚宽名。"""
    fresh = (_dt.datetime.now() - _dt.timedelta(days=1)).isoformat()
    snap = tmp_path / "etf_universe_snapshot.json"
    snap.write_text(json.dumps({
        "fetched_at": fresh,
        "codes": ["511880.XSHG"],
        "names": {"511880.XSHG": "货币ETF-A"},
        "list_dates": {},
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(jq_names, "SNAPSHOT_PATH", str(snap))
    monkeypatch.setattr(jq_names, "MAX_AGE", _dt.timedelta(days=30))
    jq_names._CACHE = None
    try:
        assert jq_names.load_jq_names().get("511880.XSHG") == "货币ETF-A"
    finally:
        jq_names._CACHE = None
