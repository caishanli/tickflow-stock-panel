"""首板选股批量取数回归：同一批数据只拉一次，不逐日/逐只重复取数。

回归（2026-10-13 实测）：09:26 选股 173s，其中首板池 3 次全市场 batch
get_price（seed 2 天 + 当天各 1 次）占 35s；deep 环 26 候选逐只
attribute_history（1d/101d）+ get_call_auction 占 43s。
"""
import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

FIXTURE = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "shoubangaokai"
    / "strategy.py"
)

SEP28 = datetime.date(2026, 9, 28)
SEP29 = datetime.date(2026, 9, 29)
SEP30 = datetime.date(2026, 9, 30)


def _load_fns(ns, *names):
    import ast

    src = FIXTURE.read_text(encoding="utf-8")
    mod = ast.parse(src)
    for name in names:
        node = next(
            n for n in mod.body
            if isinstance(n, ast.FunctionDef) and n.name == name
        )
        fn_src = ast.get_source_segment(src, node)
        assert fn_src, f"fixture 缺少 {name}"
        exec(compile(fn_src, str(FIXTURE), "exec"), ns)
    return ns


def _hl_frame():
    """3 天 canned 日线（列仿真实 batch 输出：code/time/close/high_limit/paused）。"""
    rows = []

    def bar(code, day, close, high_limit, paused=0):
        rows.append({"code": code, "time": pd.Timestamp(day),
                     "close": close, "high_limit": high_limit, "paused": paused})

    bar("STOCK_Z", SEP28, 10.0, 10.0)
    bar("NOISE", SEP28, 9.0, 10.0)            # 未涨停
    bar("PAUSED_L", SEP28, 10.0, 10.0, paused=1)  # 涨停但停牌，须排除
    bar("STOCK_A", SEP29, 10.0, 10.0)
    bar("STOCK_B", SEP29, 10.0, 10.0)
    bar("STOCK_B", SEP30, 11.0, 11.0)
    bar("STOCK_C", SEP30, 11.0, 11.0)
    return pd.DataFrame(rows)


def _make_ns(calls):
    g = SimpleNamespace(l0=SimpleNamespace(n_days_limit_up_list=[]))
    log_lines: list = []
    log = SimpleNamespace(
        info=lambda msg, *a, **k: log_lines.append(("info", msg)),
        warning=lambda msg, *a, **k: log_lines.append(("warning", msg)),
    )
    frame = _hl_frame()

    def get_price(stock_list, end_date=None, count=None, **kw):
        calls.append((list(stock_list) if isinstance(stock_list, list) else stock_list,
                      str(end_date), count, kw.get("fields")))
        # 仿引擎窗口语义：取 ≤end_date 的最近 count 个 distinct 日期
        end = pd.Timestamp(end_date).normalize() if end_date else frame["time"].max()
        days = sorted(frame["time"].dt.normalize().unique())
        days = [d for d in days if d <= end][-count:] if count else [d for d in days if d <= end]
        return frame[frame["time"].dt.normalize().isin(days)].reset_index(drop=True)

    all_days = [SEP28, SEP29, SEP30]
    ns = {
        "g": g,
        "log": log,
        "pd": pd,
        "get_trade_days": lambda end_date=None, count=None: [
            d for d in all_days if d <= end_date][-count:] if count else list(all_days),
        "l0_set_stockpool": lambda ctx: ["STOCK_A", "STOCK_B", "STOCK_C", "STOCK_Z"],
        "l0_filter_kcbj_stock": lambda lst: lst,
        "l0_filter_st_paused_stock": lambda lst, today: lst,
        "l0_filter_new_stock": lambda lst, today: lst,
        "get_price": get_price,
    }
    return g, ns, log_lines


def _make_context():
    return SimpleNamespace(
        current_dt=datetime.datetime(2026, 10, 12, 9, 26),
        previous_date=SEP30,
    )


def test_prepare_seed_fetches_three_days_once(tmp_path):
    """首次 seed 的 3 天涨停只拉一次 batch（RED：现在 seed 2 次 + 当天 1 次）。"""
    calls: list = []
    g, ns, _ = _make_ns(calls)
    _load_fns(ns, "l0_prepare_stock_list", "l0_get_hl_stock", "_hl_from_frame")
    pool = ns["l0_prepare_stock_list"](_make_context())

    assert sorted(pool) == ["STOCK_C"]
    # seed 2 天 + 当天 append 后 pop(0)，窗口最终保留后 2 天（与幂等测试同口径）
    assert [sorted(w) for w in g.l0.n_days_limit_up_list] == [["STOCK_A", "STOCK_B"], ["STOCK_B", "STOCK_C"]]
    assert len(calls) == 1, calls


def _select_ns(calls, a_money=8e8):
    """l0_get_stock_list 全链路 harness：3 候选（A 全过 / B 均价额挂 / C 行情缺失）。"""
    g = SimpleNamespace(l0=SimpleNamespace(turnover_min=3, turnover_max=25.0,
                                            max_buy_num=2))
    log_lines: list = []
    log = SimpleNamespace(
        info=lambda msg, *a, **k: log_lines.append(("info", msg)),
        warning=lambda msg, *a, **k: log_lines.append(("warning", msg)),
    )
    day = pd.Timestamp("2026-10-09")
    prev_rows = [
        {"code": "A", "time": day, "close": 10.0, "volume": 1_000_000.0, "money": a_money},
        {"code": "B", "time": day, "close": 10.0, "volume": 1_000_000.0, "money": 1e8},
    ]
    hist_rows = [{"code": "A", "time": day - pd.Timedelta(days=100 - i),
                  "high": 9.0 if i < 100 else 10.0,
                  "volume": 1000.0 if i < 100 else 2000.0} for i in range(101)]

    def attribute_history(security, count, unit="1d", fields=None, **kw):
        calls.append(("attribute_history", security, count, unit, tuple(fields or [])))
        if unit != "1d":
            return pd.DataFrame()
        if count == 1:
            rows = [r for r in prev_rows if r["code"] == security]
            return pd.DataFrame(rows).set_index(pd.DatetimeIndex([day] * len(rows))) \
                if rows else pd.DataFrame()
        sub = [r for r in hist_rows if r["code"] == security]
        idx = pd.DatetimeIndex([r["time"] for r in sub])
        out = pd.DataFrame(sub).set_index(idx).drop(columns=["time", "code"])
        return out[[c for c in (fields or []) if c in out.columns]]

    def get_price(security, count=None, frequency="daily", fields=None,
                  panel=False, fq="pre", **kw):
        codes = [security] if isinstance(security, str) else list(security)
        calls.append(("get_price", sorted(codes), count, tuple(fields or [])))
        if count == 1:
            rows = [r for r in prev_rows if r["code"] in codes]
            return pd.DataFrame(rows)
        rows = [r for r in hist_rows if r["code"] in codes]
        return pd.DataFrame(rows).sort_values("time").reset_index(drop=True)

    def get_call_auction(security, start_date=None, end_date=None, fields=None, **kw):
        codes = [security] if isinstance(security, str) else list(security)
        calls.append(("get_call_auction", sorted(codes)))
        rows = [{"code": "A", "time": pd.Timestamp("2026-10-12 09:25:00"),
                 "volume": 50_000.0, "current": 10.5},
                {"code": "B", "time": pd.Timestamp("2026-10-12 09:25:00"),
                 "volume": 50_000.0, "current": 10.5}]
        return pd.DataFrame([r for r in rows if r["code"] in codes])

    def get_valuation(security, start_date=None, end_date=None, fields=None, **kw):
        calls.append(("get_valuation", security))
        return pd.DataFrame([{"turnover_ratio": 8.0, "market_cap": 100.0,
                              "circulating_market_cap": 200.0}])

    board = {"first_limit_time": "09:30", "open_board_times": 0,
             "final_reseal_time": "09:30", "reason": "ok"}
    ns = {
        "g": g,
        "log": log,
        "pd": pd,
        "datetime": datetime.datetime,
        "l0_prepare_stock_list": lambda ctx: ["A", "B", "C"],
        "attribute_history": attribute_history,
        "get_price": get_price,
        "get_call_auction": get_call_auction,
        "get_valuation": get_valuation,
        "get_security_info": lambda s: SimpleNamespace(display_name=s),
        "l0_check_yesterday_first_board_quality": lambda s, ctx: (True, dict(board)),
    }
    ctx = SimpleNamespace(
        current_dt=datetime.datetime(2026, 10, 12, 9, 26),
        previous_date=datetime.date(2026, 10, 9),
    )
    return ns, ctx, log_lines


def test_select_batches_daily_history_and_auction(tmp_path):
    """26 候选 deep 环的日线/竞价取数按批拉（RED：现在逐只 attribute_history）。"""
    calls: list = []
    ns, ctx, _ = _select_ns(calls)
    _load_fns(ns, "l0_get_stock_list", "l0_calculate_quality_score",
              "l0_time_to_minutes", "_rows_for_code")
    qualified = ns["l0_get_stock_list"](ctx)

    assert qualified == ["A"]
    kinds = [c[0] for c in calls]
    # 逐只日线历史必须消失（1d×3 + 101d×1 → 0）
    assert kinds.count("attribute_history") == 0, calls
    # 日线走两次 batch（昨日 1 天 + 左压 101 天），竞价一次 batch
    assert sorted(c[2] for c in calls if c[0] == "get_price") == [1, 101], calls
    auc = [c for c in calls if c[0] == "get_call_auction"]
    assert len(auc) == 1 and auc[0][1] == ["A", "B", "C"], calls


def test_st_filter_emits_heartbeat_on_slow_scan(tmp_path):
    """ST 过滤长循环必须有心跳：2026-10-14 两进程僵死 70 分钟零输出才被发现。

    FakeDateTime 每次 now() 步进 10s：4 只候选中第 3 只处触发一次心跳。
    """
    ns: dict = {}
    _load_fns(ns, "l0_filter_st_paused_stock")
    ticks = {"t": datetime.datetime(2026, 10, 14, 9, 26, 1)}

    class FakeDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            ticks["t"] += datetime.timedelta(seconds=10)
            return ticks["t"]

    log_lines: list = []
    ns["datetime"] = FakeDateTime
    ns["log"] = SimpleNamespace(
        info=lambda msg, *a, **k: log_lines.append(("info", msg)),
        warning=lambda msg, *a, **k: log_lines.append(("warning", msg)),
    )

    def mk(paused=False, name="平安银行"):
        return SimpleNamespace(paused=paused, name=name, is_st=False)

    ns["get_current_data"] = lambda: {
        "AAA": mk(), "ST*BBB": mk(name="*ST康美"),
        "CCC": mk(paused=True), "DDD": mk(name="退市股"),
    }
    out = ns["l0_filter_st_paused_stock"](
        ["AAA", "ST*BBB", "CCC", "DDD"], datetime.date(2026, 10, 14))

    assert out == ["AAA"]
    beats = [m for lv, m in log_lines if "选股心跳" in m]
    assert len(beats) == 1, log_lines
    assert "3/4" in beats[0] and "CCC" in beats[0], beats


def test_no_survivor_never_touches_auction(tmp_path):
    """零通过者不得拉竞价：否则未被消费的缺缓存标的会置
    preopen_auction_missing，触发盘前重试把整轮选股每分钟重跑
    （2026-10-15 实测 09:26 回调连跑 7 次，RED：提前 batch 必拉一次）。"""
    calls: list = []
    ns, ctx, _ = _select_ns(calls, a_money=1e7)
    _load_fns(ns, "l0_get_stock_list", "l0_calculate_quality_score",
              "l0_time_to_minutes", "_rows_for_code")
    qualified = ns["l0_get_stock_list"](ctx)

    assert qualified == []
    assert [c for c in calls if c[0] == "get_call_auction"] == [], calls
