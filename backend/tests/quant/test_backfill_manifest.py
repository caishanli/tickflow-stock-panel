"""backfill manifest 断点续传测试。"""
from __future__ import annotations

import json

from app.services import mootdx_service as ms


def test_manifest_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "MANIFEST_PATH", tmp_path / "m.json")
    ms._manifest_reset("stock_minute", ["a", "b", "c"], mode="full")
    ms._manifest_mark_done("stock_minute", ["a", "b"])
    assert ms._manifest_done("stock_minute") == {"a", "b"}
    raw = json.loads((tmp_path / "m.json").read_text())
    assert raw["stock_minute"]["mode"] == "full"


def test_manifest_reset_clears_stale_done(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "MANIFEST_PATH", tmp_path / "m.json")
    ms._manifest_reset("stock_minute", ["x"], mode="recent")
    ms._manifest_mark_done("stock_minute", ["x"])
    ms._manifest_reset("stock_minute", ["y", "z"], mode="full")  # 新一轮清空 done
    assert ms._manifest_done("stock_minute") == set()


def test_manifest_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "MANIFEST_PATH", tmp_path / "nope.json")
    assert ms._manifest_done("stock_minute") == set()
