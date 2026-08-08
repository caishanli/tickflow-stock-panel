"""ETF 名称清洗回归测试（P0 对齐修复：启用 + 扩展 _clean_etf_name）。

覆盖：
- 'AH' 清洗：银行AH价格优选ETF(517900) 不再含 'H'，避免误入策略香港组。
- 噪声词清洗：主题/产业/精选/龙头/ETF 等，对齐聚宽短名。
- 特殊组关键词保留：恒生/港股/H股/中概/恒指 不被误伤。
- 不删除数字（30/50/100/300...）：避免 创业板50ETF 等被误除导致本地/聚宽
  exclude 与分组不一致。
- 幂等：清洗结果再清洗不变。
- _clean_etf_name 保留但不再被 _load_etf_universe 调用（最终评审 I1：回测侧
  移除二次清洗，名称直接用快照/网络原始名对齐模拟盘 jq 源）。
"""
import datetime as _dt
import json

import pytest

from app.quant import rqalpha_bridge as bridge


def test_clean_removes_ah():
    assert "H" not in bridge._clean_etf_name("银行AH价格优选ETF")


def test_clean_removes_noise_words():
    assert bridge._clean_etf_name("人工智能主题ETF") == "人工"
    assert bridge._clean_etf_name("科技龙头ETF") == "科技"
    assert "产业" not in bridge._clean_etf_name("华富人工智能产业ETF")


def test_clean_preserves_special_keywords():
    assert "恒生" in bridge._clean_etf_name("恒生科技ETF")
    assert "港股" in bridge._clean_etf_name("港股通红利ETF")
    assert "H股" in bridge._clean_etf_name("H股ETF")
    assert "中概" in bridge._clean_etf_name("中概互联网ETF")
    assert "恒指" in bridge._clean_etf_name("恒指ETF")


def test_clean_does_not_remove_numbers():
    assert "50" in bridge._clean_etf_name("创业板50ETF")
    assert "300" in bridge._clean_etf_name("沪深300ETF")


def test_clean_does_not_garb_latin_names():
    assert "B" in bridge._clean_etf_name("伊塔乌巴西IBOVESPAETF")


def test_clean_idempotent():
    for n in ["银行AH价格优选ETF", "恒生科技ETF", "中概互联网ETF",
              "人工智能主题ETF", "创业板50ETF", "H股ETF", "银行ETF"]:
        once = bridge._clean_etf_name(n)
        assert bridge._clean_etf_name(once) == once


def test_clean_empty_unchanged():
    assert bridge._clean_etf_name("") == ""
    assert bridge._clean_etf_name(None) is None


class _UniverseDM:
    """_load_etf_universe 需要的最小 DataManager 鸭子类型。"""

    def __init__(self, network=None, cache_codes=()):
        self._daily_mem = {"get_daily_" + c: object() for c in cache_codes}
        self.sources = {"network": network}


class _FakeNetworkSrc:
    def __init__(self, names):
        self._names = names

    def get_stock_names(self):
        return self._names


def _write_snapshot(path, codes=("517900.XSHG", "512800.XSHG"), days_ago=0,
                    names=None):
    names = names or {"517900.XSHG": "银行AH价格优选ETF",
                      "512800.XSHG": "银行ETF"}
    payload = {
        "fetched_at": (_dt.datetime.now() - _dt.timedelta(days=days_ago)).isoformat(),
        "codes": list(codes),
        "names": names,
        "list_dates": {c: ["2020-01-01", "2999-12-31"] for c in codes},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_load_etf_universe_uses_raw_snapshot_names(tmp_path, monkeypatch):
    """快照命中路径不再二次清洗：名称 = 快照原始聚宽名。"""
    snap = tmp_path / "etf_universe_snapshot.json"
    _write_snapshot(snap)
    monkeypatch.setattr(bridge, "_ETF_UNIVERSE_SNAPSHOT", str(snap))
    dm = _UniverseDM()
    codes, names, list_dates = bridge._load_etf_universe(dm)
    assert names["517900.XSHG"] == "银行AH价格优选ETF"
    assert "H" in names["517900.XSHG"]


def test_load_etf_universe_uses_raw_derived_names(tmp_path, monkeypatch):
    """快照缺失：网络名转 JQ 码键原样返回（不经 _clean_etf_name）。"""
    monkeypatch.setattr(bridge, "_ETF_UNIVERSE_SNAPSHOT",
                        str(tmp_path / "nonexistent.json"))
    dm = _UniverseDM(network=_FakeNetworkSrc(
        {"517900": "银行AH价格优选ETF"}), cache_codes=["517900.XSHG"])
    codes, names, list_dates = bridge._load_etf_universe(dm)
    assert "517900.XSHG" in names
    assert names["517900.XSHG"] == "银行AH价格优选ETF"
