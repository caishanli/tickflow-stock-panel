"""wufu-v5.4-ding-report 策略验证：语法 + 预买卖报告组装/排序/守卫。"""
import py_compile
import sys
import types
from datetime import datetime
from pathlib import Path

STRATEGY = Path(__file__).parent.parent / "fixtures" / "wufu_v54" / "wufu-v5.4-ding-report.py"


def test_strategy_compiles():
    assert STRATEGY.exists()
    py_compile.compile(str(STRATEGY), doraise=True)


def test_strategy_registers_two_pre_reports_and_quiet_call():
    src = STRATEGY.read_text(encoding="utf-8")
    assert "run_daily(pre_trade_report, time='11:30')" in src
    assert "run_daily(pre_trade_report, time='13:01')" in src
    assert "get_final_ranked_etfs(context, quiet=True)" in src
    assert src.count("if not quiet:") >= 4


class _FakeLog:
    def __init__(self):
        self.notifies, self.infos, self.warnings = [], [], []

    def info(self, msg):
        self.infos.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def error(self, msg):
        pass

    def set_level(self, *a):
        pass

    def notify(self, msg):
        self.notifies.append(msg)


def _load_strategy():
    fake_jq = types.ModuleType("jqdata")  # 规避 from jqdata import *
    sys.modules["jqdata"] = fake_jq
    ns = {"__name__": "wufu_ding_report_test", "log": _FakeLog(),
          "g": types.SimpleNamespace(), "get_trade_days": lambda **k: []}
    exec(compile(STRATEGY.read_text(encoding="utf-8"), str(STRATEGY), "exec"), ns)
    return ns


def _m(etf, name, score):
    return {"etf": etf, "etf_name": name, "momentum_score": score}


def _make_ctx(positions, dt=datetime(2026, 8, 25, 13, 1)):
    port = types.SimpleNamespace(
        positions={code: types.SimpleNamespace(total_amount=amt)
                   for code, amt in positions.items()})
    ctx = types.SimpleNamespace(portfolio=port, current_dt=dt)
    return ctx


NAMES = {"A.XSHG": "甲ETF", "B.XSHG": "乙ETF", "C.XSHE": "丙ETF", "D.XSHG": "丁ETF",
         "E.XSHE": "戊ETF", "F.XSHE": "己ETF", "511880.XSHG": "银华日利"}
FULL = [_m("A.XSHG", "甲ETF", 2.0), _m("B.XSHG", "乙ETF", 1.8), _m("C.XSHE", "丙ETF", 1.6),
        _m("D.XSHG", "丁ETF", 1.4), _m("E.XSHE", "戊ETF", 1.2), _m("F.XSHE", "己ETF", 1.0)]
ASSESS = {"A.XSHG", "B.XSHG", "C.XSHE", "D.XSHG", "E.XSHE", "F.XSHE"}


def _prep(ns, ranked_full=None, assessed=None, holdings_num=3,
          defensive_ok=True, pool=("A.XSHG", "B.XSHG", "C.XSHE", "E.XSHE", "F.XSHE")):
    g = ns["g"]
    g.holdings_num = holdings_num
    g.is_a_share_weak = False
    g.merged_etf_pool = list(pool)
    g.defensive_etf = "511880.XSHG"
    g.ranked_candidates_full = FULL if ranked_full is None else ranked_full
    g._assessed_codes = ASSESS if assessed is None else assessed
    ns["get_security_name"] = lambda c: NAMES.get(c, c)
    ns["check_defensive_etf_available"] = lambda ctx: defensive_ok


def test_report_predicts_buy_and_sell_order():
    ns = _load_strategy()
    _prep(ns)
    ctx = _make_ctx({"C.XSHE": 1000, "E.XSHE": 2000})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", FULL[:3])
    assert msg.startswith("📋 预买卖报告 08-25 13:01")
    assert "🟢 大A正常期 | 池5只" in msg
    assert "📥 预计买入：A 甲ETF → B 乙ETF" in msg          # 目标 A,B,C 减持仓 C
    assert "📤 预计卖出（最可能前1）：" in msg
    assert "1️⃣ E 戊ETF（排名5/6，动量1.2000）" in msg       # E 持仓不在目标


def test_report_lists_top2_candidates():
    """🎯 过滤后候选前2：不管是否持仓/是否目标都按过滤排名列出（coverage@2=100%）。"""
    ns = _load_strategy()
    _prep(ns)
    ctx = _make_ctx({"C.XSHE": 1000, "E.XSHE": 2000})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", FULL)
    assert "🎯 过滤后候选前2：A 甲ETF → B 乙ETF" in msg
    # 持仓 E 也应出现在候选里（候选与买卖预测独立）
    ctx2 = _make_ctx({"E.XSHE": 2000})
    msg2 = ns["_build_pre_trade_message"](ctx2, "11:30", [FULL[0]])
    assert "🎯 过滤后候选前2：A 甲ETF → B 乙ETF" in msg2
    assert "📥 预计买入：A 甲ETF" in msg2                     # 目标 A 减持仓 E → 买 A
    # 排名为空（防御模式）时不列候选
    ns2 = _load_strategy()
    _prep(ns2, ranked_full=[], assessed=set())
    msg3 = ns2["_build_pre_trade_message"](_make_ctx({}), "13:01", [])
    assert "🎯" not in msg3


def test_report_sell_worst_rank_first_truncates_three():
    ns = _load_strategy()
    _prep(ns, holdings_num=1)                                # 目标仅 A
    ctx = _make_ctx({"C.XSHE": 100, "D.XSHG": 200, "E.XSHE": 300, "F.XSHE": 400})
    msg = ns["_build_pre_trade_message"](ctx, "11:30", [FULL[0]])
    assert "📤 预计卖出（最可能前3）：" in msg
    lines = [ln for ln in msg.splitlines() if ln.startswith(("1️⃣", "2️⃣", "3️⃣"))]
    assert len(lines) == 3
    assert "F 己ETF" in lines[0] and "排名6/6" in lines[0]    # 排名最差者第1：F,E,D
    assert "D 丁ETF" in lines[2] and "排名4/6" in lines[2]
    assert "乙ETF" not in "".join(lines)                      # B 未持有，不在卖出候选（可在🎯候选行出现）


def test_report_no_sell_when_all_in_targets():
    ns = _load_strategy()
    _prep(ns)
    ctx = _make_ctx({"A.XSHG": 1000, "B.XSHG": 2000, "C.XSHE": 3000})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", FULL)
    assert "✅ 持仓全部在目标内（3只），13:10 预计不动" in msg
    assert "📤" not in msg and "📥 预计买入：无" in msg


def test_report_defensive_mode_when_ranked_empty():
    ns = _load_strategy()
    _prep(ns, ranked_full=[], assessed=set())
    ctx = _make_ctx({"C.XSHE": 500, "E.XSHE": 600})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", [])
    assert "🛡️ 排名为空：走防御模式" in msg
    assert "📥 预计买入：511880 银华日利" in msg
    assert "1️⃣ C 丙ETF（未入过滤排名，动量N/A）" in msg       # 无排名者按序列出
    assert "2️⃣ E 戊ETF" in msg
    assert "🎯" not in msg                                    # 防御模式不列候选（ranked_candidates_full 是昨日残留）


def test_report_unevaluated_holding_listed_separately():
    ns = _load_strategy()
    _prep(ns, assessed=ASSESS - {"F.XSHE"})                  # F 在池内但未参与评估
    ctx = _make_ctx({"C.XSHE": 500, "F.XSHE": 600})
    msg = ns["_build_pre_trade_message"](ctx, "13:01", FULL)
    assert "⚠️ 未参与评估（数据缺失保护，13:10 不会强卖）：F.XSHE 己ETF" in msg
    assert "📤" not in msg                                    # F 不进卖出候选


def test_pre_trade_report_replay_guard_skips_past_days():
    ns = _load_strategy()
    _prep(ns)
    from datetime import timedelta
    ctx_past = _make_ctx({"A.XSHG": 100}, dt=datetime.now() - timedelta(days=5))  # 历史日
    ns["pre_trade_report"](ctx_past)
    assert not ns["log"].notifies                             # 过去日期补跑直接跳过
    ctx_today = _make_ctx({"A.XSHG": 100}, dt=datetime.now() - timedelta(minutes=30))
    assert ns["_is_replay_past"](ctx_today) is False           # 今天的盘中补跑允许触发


def test_pre_trade_report_pool_rebuild_fail_skips():
    ns = _load_strategy()
    ns["g"].__dict__.pop("merged_etf_pool", None)
    ns["check_a_share_weak_period"] = lambda ctx: (_ for _ in ()).throw(RuntimeError("boom"))
    from datetime import timedelta
    ctx = _make_ctx({}, dt=datetime.now() - timedelta(minutes=1))  # 今天盘中，非历史补跑
    ns["pre_trade_report"](ctx)
    assert not ns["log"].notifies
    assert any("池重建失败" in m for m in ns["log"].warnings)
