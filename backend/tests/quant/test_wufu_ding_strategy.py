"""wufu-v5.4-ding 策略文件验证：语法 + 买卖路径含 log.notify + 通知文案执行级验证。"""
import py_compile
import sys
import types
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

STRATEGY = Path(__file__).parent.parent / "fixtures" / "wufu_v54" / "wufu-v5.4-ding.py"


def test_strategy_compiles():
    assert STRATEGY.exists()
    py_compile.compile(str(STRATEGY), doraise=True)


def test_strategy_has_notify_on_buy_and_sell():
    src = STRATEGY.read_text(encoding="utf-8")
    assert "log.notify" in src
    assert "📥 买入" in src
    assert "📤 卖出" in src
    assert "g._entry_date" in src
    assert "_notify_trade" in src


def test_strategy_has_no_rebalance_notify():
    """当日无换仓时 buy_routine 也发 log.notify（g._daily_traded 门控）。"""
    src = STRATEGY.read_text(encoding="utf-8")
    assert "g._daily_traded" in src
    assert "今日无换仓" in src


class _FakeLog:
    """桩 log：记录 notify/info/error/warning 调用（引擎 LogProxy 的最小替身）。"""

    def __init__(self):
        self.notifies = []
        self.infos = []
        self.errors = []
        self.warnings = []

    def info(self, msg):
        self.infos.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def error(self, msg):
        self.errors.append(msg)

    def notify(self, msg):
        self.notifies.append(msg)


def _load_strategy():
    """在桩命名空间里执行策略脚本，返回 ns（可调用其 _notify_trade/_holding_trade_days）。"""
    fake_jq = types.ModuleType("jqdata")  # 规避 `from jqdata import *`（引擎运行时提供，测试环境无）
    sys.modules["jqdata"] = fake_jq
    ns = {
        "__name__": "wufu_v54_ding_test",
        "log": _FakeLog(),
        "g": types.SimpleNamespace(_entry_date={}),
        "get_trade_days": lambda start_date=None, end_date=None: [],
    }
    exec(compile(STRATEGY.read_text(encoding="utf-8"), str(STRATEGY), "exec"), ns)
    return ns


def _ctx(day):
    c = types.SimpleNamespace()
    c.current_dt = types.SimpleNamespace()
    c.current_dt.date = lambda: day
    return c


def test_notify_trade_sell_matches_trade_record():
    """卖出通知文案对齐 ddd911f4 真实成交（159518 07-17买 1.129 → 07-21卖 1.163）。"""
    ns = _load_strategy()
    ns["g"]._entry_date["159518.XSHE"] = date(2026, 7, 17)
    ns["get_trade_days"] = lambda start_date=None, end_date=None: [
        date(2026, 7, 17), date(2026, 7, 20), date(2026, 7, 21)]
    ns["_notify_trade"]("159518.XSHE", "标普油气ETF嘉实", "卖出", 88000, 1.163, 1.129,
                        _ctx(date(2026, 7, 21)))
    assert ns["log"].notifies, "卖出应产生 notify"
    msg = ns["log"].notifies[-1]
    assert "📤 卖出 标普油气ETF嘉实(159518.XSHE) 数量88000 价格1.163" in msg
    assert "佣金10.23" in msg
    assert "盈利+2992.00(+3.01%)" in msg
    assert "持仓2个交易日" in msg


def test_notify_trade_buy_format():
    """买入通知含名称/代码/数量/价格/佣金。"""
    ns = _load_strategy()
    ns["_notify_trade"]("159985.XSHE", "豆粕ETF华夏", "买入", 46800, 2.132, 0.0,
                        _ctx(date(2026, 7, 17)))
    assert ns["log"].notifies, "买入应产生 notify"
    msg = ns["log"].notifies[-1]
    assert "📥 买入 豆粕ETF华夏(159985.XSHE) 数量46800 价格2.132" in msg


# ---- ETF 成交额异常日判定 ----

def test_anomalous_etf_days_detects_low_count():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4.2e11, 1.5e11], index=idx)          # 08-13 金额仅 ~36%
    counts = pd.Series([1658, 1657, 225], index=idx)               # 08-13 只数仅 ~13.6%
    assert ns["_anomalous_etf_days"](totals, counts) == [idx[2]]


def test_anomalous_etf_days_no_false_positive():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4.2e11, 4.6e11], index=idx)
    counts = pd.Series([1658, 1657, 1658], index=idx)
    assert ns["_anomalous_etf_days"](totals, counts) == []


def test_anomalous_etf_days_exactly_50pct_is_ok():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4e11, 2e11], index=idx)              # 恰好 50%
    counts = pd.Series([1658, 1658, 829], index=idx)               # 恰好 50%
    assert ns["_anomalous_etf_days"](totals, counts) == []


def test_anomalous_etf_days_detects_low_money():
    ns = _load_strategy()
    idx = pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"])
    totals = pd.Series([4e11, 4.2e11, 1.9e11], index=idx)          # ~45%，低于 50%
    counts = pd.Series([1658, 1657, 1650], index=idx)              # 只数正常
    assert ns["_anomalous_etf_days"](totals, counts) == [idx[2]]


# ---- calculate_global_etf_threshold 接入异常自检 ----

def _threshold_ctx(prev_day=date(2026, 8, 13)):
    c = types.SimpleNamespace()
    c.previous_date = prev_day
    c.current_dt = types.SimpleNamespace()
    c.current_dt.date = lambda: prev_day
    return c


def _money_df():
    return pd.DataFrame({
        "code": ["510300.XSHG"] * 3 + ["511880.XSHG"] * 3,
        "time": pd.DatetimeIndex(["2026-08-11", "2026-08-12", "2026-08-13"] * 2),
        "money": [2e11, 2.1e11, 0.75e11, 2.2e11, 2.0e11, 0.75e11],
    })


def test_threshold_excludes_anomaly_and_notifies(monkeypatch):
    """异常天被剔除，阈值用正常两天均值；log.error 进异常标签、log.notify 推钉钉。"""
    from app.quant.jqengine.datasource import manager as mgr_mod

    fake_dm = types.SimpleNamespace(
        get_daily_money_cached=lambda *a, **k: _money_df())
    monkeypatch.setattr(mgr_mod, "get_data_manager", lambda: fake_dm)
    ns = _load_strategy()
    ns["g"]._cached_etf_universe = ["510300.XSHG", "511880.XSHG"]
    ns["g"].global_threshold_divisor = 20000
    ns["get_trade_days"] = lambda end_date=None, count=0: [
        date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13)]
    ns["calculate_global_etf_threshold"](_threshold_ctx())
    # daily_totals：08-11=4.2e11, 08-12=4.1e11, 08-13=1.5e11（< 4.2e11*0.5=2.1e11 → 异常）
    # 剔除 08-13 后均值 = (4.2e11+4.1e11)/2 = 4.15e11；阈值 = 4.15e11 / 20000
    assert ns["g"].avg_etf_money_threshold == pytest.approx(4.15e11 / 20000)
    assert any("成交额异常" in m for m in ns["log"].errors)
    assert any("成交额异常" in m for m in ns["log"].notifies)


# ---- 数据缺失保护：持仓未参与动量计算时保守保留（08-17~08-21 连续误换仓回归） ----

import numpy as np


def _ranking_ns(held_code="518880.XSHG", held_in_pool=True,
                assessed=("159518.XSHE", "513030.XSHG")):
    """构造 get_final_ranked_etfs 的执行环境：
    - 池含 held_code（可关）与 assessed 中标的；
    - hist_df 只含 assessed（held 取数失败缺席 → 复现 08-21 场景）；
    - 159518 通过全部过滤得分 0.35，513030 未通过短期风控。"""
    ns = _load_strategy()
    pool = list(assessed) + ([held_code] if held_in_pool else [])
    g = ns["g"]
    g.merged_etf_pool = pool
    g.lookback_days = 25
    g.volume_lookback = 5
    g.ma_lookback = 10
    g.is_a_share_weak = True
    g.holdings_num = 1
    g.score_threshold_ratio = 0.9
    g.min_score_threshold = 0
    g.max_score_threshold = 5
    g.r2_threshold = 0.4
    g.ma_threshold = 1.0
    g.volume_threshold = 1.8
    g.loss = 0.97
    g.enable_r2_filter = False   # 走弱期：R² 关、均线开
    g.enable_ma_filter = True
    g.enable_volume_check = True
    g.enable_loss_filter = True
    g.hold_buffer = 1.0

    days = pd.date_range("2026-06-01", periods=40, freq="D")
    rows = []
    for code in assessed:
        base = 1.0 if code == "159518.XSHE" else 2.0
        for i, d in enumerate(days):
            rows.append({"time": d, "code": code, "close": base * (1 + 0.001 * i),
                         "volume": 1e6})
    ns["get_price"] = lambda *a, **k: (pd.DataFrame(rows) if k.get("frequency") != "1m"
                                       else pd.DataFrame())
    cur = {c: types.SimpleNamespace(paused=False, last_price=float(base))
           for c, base in [("159518.XSHE", 1.04), ("513030.XSHG", 2.0),
                           ("518880.XSHG", 9.3)]}
    ns["get_current_data"] = lambda: cur
    ns["is_temporarily_suspended"] = lambda *a, **k: False
    ns["get_security_name"] = lambda c: {"159518.XSHE": "标普油气ETF嘉实",
                                         "513030.XSHG": "德国ETF",
                                         "518880.XSHG": "黄金ETF"}.get(c, c)

    def fake_metrics(etf, etf_name, hist_closes, hist_volumes, current_price,
                     today_vol, context):
        passed_loss = etf != "513030.XSHG"
        return {
            "etf": etf, "etf_name": etf_name,
            "momentum_score": 0.35 if etf == "159518.XSHE" else 0.20,
            "annualized_returns": 0.4, "r_squared": 0.6,
            "current_price": current_price, "volume_ratio": 1.0,
            "day_ratios": [1.0] if passed_loss else [0.96],
            "passed_momentum": True, "passed_r2": True, "passed_ma": True,
            "passed_volume": True, "passed_loss": passed_loss, "ma_value": 1.0,
        }

    ns["calculate_all_metrics_for_etf"] = fake_metrics

    pos = types.SimpleNamespace(total_amount=12600, avg_cost=9.235)
    ctx = types.SimpleNamespace(
        previous_date=pd.Timestamp("2026-08-20"),
        current_dt=types.SimpleNamespace(date=lambda: pd.Timestamp("2026-08-21").date()),
        portfolio=types.SimpleNamespace(positions={held_code: pos}))
    return ns, ctx


def test_missing_holding_is_protected_and_retained():
    """持仓因取数失败缺席动量计算 → 保守保留 + 告警/通知，不得换仓。"""
    ns, ctx = _ranking_ns()
    result = ns["get_final_ranked_etfs"](ctx)
    assert [m["etf"] for m in result] == ["518880.XSHG"], \
        f"持仓应被数据缺失保护保留，实际最终目标: {[m['etf'] for m in result]}"
    assert any("数据缺失保护" in w and "518880" in w for w in ns["log"].warnings)
    assert any("数据缺失保护" in n for n in ns["log"].notifies), "保护动作应推钉钉"


def test_assessed_but_filtered_holding_is_not_protected():
    """持仓参与了计算但被过滤淘汰 → 正常换仓逻辑，不受保护干扰。"""
    ns, ctx = _ranking_ns(held_code="513030.XSHG")
    result = ns["get_final_ranked_etfs"](ctx)
    assert [m["etf"] for m in result] == ["159518.XSHE"]
    assert not any("数据缺失保护" in n for n in ns["log"].notifies)


def test_off_pool_holding_missing_is_not_protected():
    """防御型持仓不在合并池内（如银华日利）→ 缺席属正常，不得保护锁死。"""
    ns, ctx = _ranking_ns(held_code="511880.XSHG", held_in_pool=False)
    result = ns["get_final_ranked_etfs"](ctx)
    assert [m["etf"] for m in result] == ["159518.XSHE"]
    assert not any("数据缺失保护" in n for n in ns["log"].notifies)
