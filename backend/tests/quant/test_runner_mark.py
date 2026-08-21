"""收盘重估 + 盘中实时打标测试（离线：stub DM / feed）。"""
from __future__ import annotations

import datetime

import pandas as pd
import pytest

from app.quant import db, service
from app.quant.config import CONFIG
from app.quant.simulate import protocol, runner
from app.quant.simulate.matcher import Matcher
from app.quant.strategies.store import save_strategy


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path / "strategies"))
    db.init_db(str(db_path))
    return tmp_path


class _FakeClient:
    """盘中 feed 用：current_snapshot 返回 as_of 时刻的 1 分钟价。"""

    def __init__(self, price=12.0):
        self.price = float(price)

    def current_snapshot(self, codes, as_of=None):
        idx = pd.DatetimeIndex([pd.Timestamp(as_of)])
        return {c: pd.DataFrame({"open": [self.price], "high": [self.price],
                                 "low": [self.price], "close": [self.price],
                                 "volume": [100], "amount": [100.0]},
                                index=idx) for c in codes}


class _StubDM:
    """最小 DataManager stub：收盘价由 get_minute_price_at 提供。"""

    _minute_mem = {}
    _minute_cov = {}
    _daily_mem = {}
    _daily_ver = 0

    def __init__(self, close_price=None):
        self.client = _FakeClient()
        self.close_price = close_price

    def get_minute_price_at(self, code, dt):
        if self.close_price is None:
            return None
        if pd.Timestamp(dt).time() <= datetime.time(15, 0):
            return self.close_price
        return None

    def fetch(self, method, *a, **k):
        if method == "get_daily" and a:
            # 回退源（get_minute_price_at 无收盘价时用）：单日 close
            return pd.DataFrame({"close": [self.close_price or 0.0]},
                                index=pd.DatetimeIndex([pd.Timestamp(a[-1])]))
        raise RuntimeError("stub: no data")


STRATEGY_NOOP = "def init(context):\n    context.universe = []\n"


def _st():
    return {
        "cash": 0.0, "start_cash": 50000.0, "net_value": 50000.0, "pnl": 0.0,
        "positions": {"510300.XSHG": {"amount": 5000.0, "avg_cost": 10.0,
                                      "price": 10.0, "today_amount": 0.0}},
        "stop_loss_log": [], "dt": "2026-07-17 09:31",
    }


def _revalue_at_close_setup(tmp_quant):
    save_strategy("s_rc", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_rc", 100000.0, 0.03, "s_rc")
    protocol.save_state(aid, _st())
    return aid


def test_revalue_at_close_marks_to_real_close(tmp_quant):
    """收盘重估：持仓按当日真实收盘价打标并更新 state 净值（不落库）。"""
    aid = _revalue_at_close_setup(tmp_quant)
    dm = _StubDM(close_price=12.0)
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()},
        "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0

    runner._revalue_at_close(dm, ctx, st, pd.Timestamp("2026-07-17 15:05"))

    assert st["positions"]["510300.XSHG"]["price"] == 12.0
    assert st["net_value"] == pytest.approx(5000 * 12.0)          # 0 现金 + 市值
    assert st["pnl"] == pytest.approx(5000 * 12.0 - 50000.0)
    # 重估后 pos.price 也同步（供后续 matcher/persist 用）
    assert ctx.portfolio.positions["510300.XSHG"].price == 12.0


def test_eod_revalues_positions_to_close(tmp_quant, monkeypatch):
    """_eod 终态快照按当日收盘价打标，而非最后 feed 价。"""
    aid = _revalue_at_close_setup(tmp_quant)
    dm = _StubDM(close_price=12.0)
    bundle = type("B", (), {"daily": [], "after_trading_end": None})()
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()}, "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0
    runner._state_from_portfolio(ctx, st)
    monkeypatch.setattr(runner, "_safe_call", lambda *a, **k: None)
    runner._eod(aid, bundle, ctx, dm, st, {"fresh_frames": {}, "jq_api": object()},
                datetime.datetime(2026, 7, 17, 15, 5))

    snaps = db.get_sim_snapshots(aid)
    assert snaps[-1]["positions_value"] == pytest.approx(5000 * 12.0)
    assert snaps[-1]["net_value"] == pytest.approx(5000 * 12.0)
    assert protocol.read_state(aid)["positions"]["510300.XSHG"]["price"] == 12.0


def test_replay_today_after_close_revalues(tmp_quant, monkeypatch):
    """收盘后补跑今天：末快照按真实收盘价，而非停在买入价。"""
    class _FixedNow(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 8, 7, 17, 0, 0)

    monkeypatch.setattr(runner.datetime, "datetime", _FixedNow)
    today = datetime.date(2026, 8, 7)
    save_strategy("s_rc2", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_rc2", 100000.0, 0.03, "s_rc2", str(today))
    protocol.save_state(aid, _st())
    dm = _StubDM(close_price=12.0)
    pauses = iter([False] * 3 + [True])  # 今天补跑逐 bar 暂停检查 + 实时段退出
    monkeypatch.setattr(runner, "is_paused", lambda aid: next(pauses))
    monkeypatch.setattr(runner, "in_trading", lambda now=None: False)
    monkeypatch.setattr(runner, "_is_trading_day", lambda dm, today: True)
    monkeypatch.setattr(runner, "_prev_close_dm", lambda dm, code, today, conv=None: None)
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)

    class _ReplayDM(_StubDM):
        def __init__(self):
            super().__init__(close_price=12.0)
            # 盘中各 bar 无分钟价（模拟 517520 不在预取池 → _hist_feed 取不到价），
            # 只有 15:00 收盘价可取 —— 复现「补跑后 pos.price 停在买入价」的真实场景
        def get_minute_price_at(self, code, dt):
            if pd.Timestamp(dt).time() == datetime.time(15, 0):
                return self.close_price
            return None

    runner.run_loop(aid, dm=_ReplayDM(), feed=None, matcher=Matcher(0.03))

    snaps = db.get_sim_snapshots(aid)
    today_snaps = [s for s in snaps if s["dt"].startswith(str(today))]
    assert today_snaps and today_snaps[-1]["positions_value"] == pytest.approx(5000 * 12.0)
    assert protocol.read_state(aid)["positions"]["510300.XSHG"]["price"] == 12.0


def _feed(price):
    def _fe(dm, codes, now, acc):
        ts = str(pd.Timestamp(now))
        return {c: price for c in codes}, pd.Timestamp(now), {c: ts for c in codes}
    return _fe


def test_mark_to_market_updates_state_and_returns_dirty(tmp_quant):
    """盘中 mark：刷新持仓价到最新、重算净值，价格跳变返回 True。"""
    aid = _revalue_at_close_setup(tmp_quant)
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()}, "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0
    runner._state_from_portfolio(ctx, st)
    last_mark = {"510300.XSHG": 10.0}

    dirty = runner._mark_to_market(_feed(12.0), None, ctx, st, last_mark,
                                   pd.Timestamp("2026-07-17 10:30"))

    assert dirty is True
    assert st["positions"]["510300.XSHG"]["price"] == 12.0
    assert st["net_value"] == pytest.approx(5000 * 12.0)
    assert last_mark["510300.XSHG"] == 12.0
    assert ctx.portfolio.positions["510300.XSHG"].price == 12.0


def test_mark_to_market_no_change_is_clean(tmp_quant):
    """价格未越过阈值时不 dirty（不落快照）。"""
    aid = _revalue_at_close_setup(tmp_quant)
    st = protocol.read_state(aid)
    from app.quant.jqengine.engine.jq.context import Position
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position()}, "cash": 0.0})()})()
    ctx.portfolio.positions["510300.XSHG"].amount = 5000.0
    ctx.portfolio.positions["510300.XSHG"].avg_cost = 10.0
    ctx.portfolio.positions["510300.XSHG"].price = 10.0
    runner._state_from_portfolio(ctx, st)
    last_mark = {"510300.XSHG": 10.0}

    dirty = runner._mark_to_market(_feed(10.0), None, ctx, st, last_mark,
                                   pd.Timestamp("2026-07-17 10:30"))

    assert dirty is False
    assert st["positions"]["510300.XSHG"]["price"] == 10.0


def test_hist_feed_falls_back_to_current_snapshot_when_minute_empty(tmp_quant):
    """补跑 feed：get_minute 无当日分区数据（服务重启/分区未落盘）时，
    回退 current_snapshot 实时兜底取当日真实价，而非整批跳过。"""
    dm = _StubDM(close_price=None)          # get_minute_price_at 恒 None
    dm.client = _FakeClient(price=12.0)     # current_snapshot 兜底源
    now = pd.Timestamp(datetime.datetime.now().date()).replace(hour=10, minute=49)

    prices, bar_dt, price_ts = runner._hist_feed(dm, ["510300.XSHG"], now, {})

    assert prices["510300.XSHG"] == 12.0
    assert bar_dt is not None
    assert price_ts["510300.XSHG"] == str(now)   # 兜底快照 bar = as_of


def test_hist_feed_skips_fallback_for_historical_day(tmp_quant):
    """补跑 feed：历史日（分区应已存在）不触发 current_snapshot 兜底，
    避免把今日实时价错配到历史 bar 上。"""
    dm = _StubDM(close_price=None)
    dm.client = _FakeClient(price=12.0)
    now = pd.Timestamp("2026-07-16 10:49:00")   # 非今日

    prices, bar_dt, price_ts = runner._hist_feed(dm, ["510300.XSHG"], now, {})

    assert prices == {}
    assert bar_dt is None
    assert price_ts == {}


def test_strategy_loop_marks_positions_during_lunch_break(tmp_quant, monkeypatch):
    """午休（11:30-13:00）持仓按最后可用价打标：补跑遇数据空洞后，
    净值不再停在上午旧价，复市前即可纠正。"""
    save_strategy("s_lunch", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_lunch", 100000.0, 0.03, "s_lunch")
    st = _st()
    st["dt"] = "2026-08-10 10:48:00"   # 今天上午，需补跑
    protocol.save_state(aid, st)
    pauses = iter([False] * 10 + [True])
    monkeypatch.setattr(runner, "is_paused", lambda aid: next(pauses))
    monkeypatch.setattr(runner, "in_trading", lambda now=None: False)   # 午休非交易
    monkeypatch.setattr(runner, "_is_trading_day", lambda dm, today: True)
    monkeypatch.setattr(runner, "_prev_close_dm", lambda dm, code, today, conv=None: None)
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)

    class _FixedNow(datetime.datetime):
        current = datetime.datetime(2026, 8, 10, 12, 0, 0)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(runner.datetime, "datetime", _FixedNow)

    class _NoDataDM(_StubDM):
        """补跑期间完全无数据（get_minute_price_at 与 current_snapshot 均空），
        复现 stock data 服务重启后当日分区未落盘的竞态。"""

        class _EmptyClient:
            def current_snapshot(self, codes, as_of=None):
                return {}

        def __init__(self):
            super().__init__(close_price=None)
            self.client = self._EmptyClient()

    runner.run_loop(aid, dm=_NoDataDM(), feed=_feed(12.0), matcher=Matcher(0.03))

    st = protocol.read_state(aid)
    assert st["positions"]["510300.XSHG"]["price"] == 12.0
    assert st["net_value"] == pytest.approx(5000 * 12.0)
    snaps = db.get_sim_snapshots(aid)
    assert snaps and snaps[-1]["positions_value"] == pytest.approx(5000 * 12.0)


def test_strategy_loop_live_marks_positions(tmp_quant, monkeypatch):
    """盘中实时：两次 tick 之间持仓价随最新行情打标，净值跟涨。"""
    save_strategy("s_mk", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_mk", 100000.0, 0.03, "s_mk")
    protocol.save_state(aid, _st())
    pauses = iter([False] * 20 + [True])
    monkeypatch.setattr(runner, "is_paused", lambda aid: next(pauses))
    monkeypatch.setattr(runner, "in_trading", lambda now=None: True)
    monkeypatch.setattr(runner, "_is_trading_day", lambda dm, today: True)
    monkeypatch.setattr(runner, "_prev_close_dm", lambda dm, code, today, conv=None: None)
    sleep_log = []
    monkeypatch.setattr(runner.time, "sleep", lambda s: sleep_log.append(s))

    class _FixedNow(datetime.datetime):
        current = datetime.datetime(2026, 7, 17, 10, 30, 0)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(runner.datetime, "datetime", _FixedNow)

    class _LiveDM(_StubDM):
        def __init__(self):
            super().__init__()
            self.client = _FakeClient(price=12.0)

    runner.run_loop(aid, dm=_LiveDM(), feed=None, matcher=Matcher(0.03))

    st = protocol.read_state(aid)
    assert st["positions"]["510300.XSHG"]["price"] == 12.0
    assert st["net_value"] == pytest.approx(5000 * 12.0)
    snaps = db.get_sim_snapshots(aid)
    assert snaps and snaps[-1]["positions_value"] == pytest.approx(5000 * 12.0)


def test_state_roundtrip_preserves_price_ts(tmp_quant):
    """positions_json 序列化/恢复保留逐股行情时间 price_ts。"""
    from app.quant.jqengine.engine.jq.context import Position

    aid = _revalue_at_close_setup(tmp_quant)
    st = protocol.read_state(aid)
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position(amount=5000.0, avg_cost=10.0,
                                              price=12.0,
                                              price_ts="2026-07-17 10:31")},
        "cash": 0.0})()})()

    runner._state_from_portfolio(ctx, st)
    assert st["positions"]["510300.XSHG"]["price_ts"] == "2026-07-17 10:31"

    ctx2 = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {}, "cash": 0.0})()})()
    runner._restore_portfolio(ctx2, st)
    assert ctx2.portfolio.positions["510300.XSHG"].price_ts == "2026-07-17 10:31"
