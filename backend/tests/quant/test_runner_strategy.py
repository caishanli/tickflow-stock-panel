"""策略驱动模拟盘主循环测试（离线：stub DataManager/feed，patch 时钟与暂停开关）。"""
from __future__ import annotations

import datetime

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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


@pytest.fixture
def api_client(tmp_quant):
    from app.quant.api.quant import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class _FakeClient:
    """最小网络客户端 stub：盘中 feed 与交易日判定可安全命中。"""

    def current_snapshot(self, codes, as_of=None):
        idx = pd.DatetimeIndex([pd.Timestamp(as_of)])
        return {c: pd.DataFrame({"open": [1.0], "high": [1.0], "low": [1.0],
                                 "close": [1.0], "volume": [100], "amount": [100.0]},
                                index=idx) for c in codes}

    def get_price(self, security, start_date=None, end_date=None, frequency="daily",
                  fields=None):
        return {}


class _StubDM:
    """最小 DataManager stub：策略下单价由 feed 的 minute_prices 快照提供。"""
    sources = {}
    _daily_mem = {}
    _minute_mem = {}
    _minute_cov = {}
    _offline = False
    _daily_ver = 0

    def __init__(self):
        self.client = _FakeClient()

    def fetch(self, method, *a, **k):
        raise RuntimeError("stub: no data")

    def get_minute_price_at(self, code, dt):
        return None

    def preload_daily(self):
        pass


STRATEGY_BUY = '''
def init(context):
    context.universe = ["510300.XSHG"]

def rebalance(context):
    order_target_percent("510300.XSHG", 0.5)
    log.info("rebalanced")

run_daily(rebalance, "open")
'''

STRATEGY_NOOP = "def init(context):\n    context.universe = []\n"

STRATEGY_HD = '''
def init(context):
    context.universe = ["510300.XSHG"]
    g.count = 0

def handle_data(context):
    g.count += 1
'''


def _today_bar(hour=10, minute=30):
    """今日盘中某时刻的 bar（与测试运行时刻无关，保证 'open'/'HH:MM' 调度确定触发）。"""
    return pd.Timestamp.now().replace(hour=hour, minute=minute, second=0, microsecond=0)


def _feed_factory(price=10.0, bar=None):
    def _feed(dm, codes, now, acc):
        return {c: price for c in codes}, (bar or _today_bar())
    return _feed


def _patch_one_loop(monkeypatch, ticks=1, pause_checks_before_loop=0):
    """让主循环跑 ticks 轮后退出；pause_checks_before_loop 预留给补跑逐日暂停检查。"""
    pauses = iter([False] * pause_checks_before_loop + [False] * ticks + [True])
    monkeypatch.setattr(runner, "is_paused", lambda aid: next(pauses))
    monkeypatch.setattr(runner, "in_trading", lambda now=None: True)
    monkeypatch.setattr(runner, "_is_trading_day", lambda dm, today: True)
    monkeypatch.setattr(runner, "_prev_close_dm", lambda dm, code, today: None)
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)


# ---- 策略驱动主循环 ----
def test_strategy_loop_buys_and_persists(tmp_quant, monkeypatch):
    save_strategy("s1", "s", STRATEGY_BUY)
    aid = service.account_create("acct", 100000.0, 0.03, "s1")
    _patch_one_loop(monkeypatch)
    runner.run_loop(aid, dm=_StubDM(), feed=_feed_factory(10.0), matcher=Matcher(0.03))

    trades = db.get_sim_trades(aid)
    assert len(trades) == 1
    assert trades[0]["action"] == "BUY" and trades[0]["code"] == "510300.XSHG"
    assert trades[0]["amount"] == 5000                       # 半仓 5 万 / 10 元，整手
    assert trades[0]["price"] == pytest.approx(10.01)      # 滑点买入价
    snaps = db.get_sim_snapshots(aid)
    assert snaps and snaps[-1]["positions_value"] == pytest.approx(50000.0)
    assert snaps[-1]["net_value"] == pytest.approx(100000.0 - 5000 * 10.01
                                                   - round(5000 * 10.01 * 0.0003, 2) + 50000.0)
    st = protocol.read_state(aid)
    assert st["positions"]["510300.XSHG"]["amount"] == 5000
    assert st["positions"]["510300.XSHG"]["today_amount"] == 5000   # T+1 冻结落库
    logs = db.get_sim_logs(aid)
    assert any("启动" in l["message"] for l in logs)
    assert any("rebalanced" in l["message"] for l in logs)  # 策略 log.info 经 sink 落库
    assert db.get_sim_account(aid)["status"] == "paused"


def test_strategy_loop_same_bar_not_refired(tmp_quant, monkeypatch):
    save_strategy("s_hd", "s", STRATEGY_HD)
    aid = service.account_create("acct_hd", 100000.0, 0.03, "s_hd")
    _patch_one_loop(monkeypatch, ticks=2)
    runner.run_loop(aid, dm=_StubDM(), feed=_feed_factory(10.0, _today_bar()),
                    matcher=Matcher(0.03))
    from app.quant.jqengine.engine.jq import api
    assert api._state["ctx"].g.count == 1                  # 同一 bar handle_data 只触发一次
    assert db.get_sim_trades(aid) == []


def test_strategy_loop_new_bar_refires(tmp_quant, monkeypatch):
    save_strategy("s_hd2", "s", STRATEGY_HD)
    aid = service.account_create("acct_hd2", 100000.0, 0.03, "s_hd2")
    _patch_one_loop(monkeypatch, ticks=2)
    bars = iter([_today_bar(), _today_bar(minute=31)])

    def _feed(dm, codes, now, acc):
        return {c: 10.0 for c in codes}, next(bars)

    runner.run_loop(aid, dm=_StubDM(), feed=_feed, matcher=Matcher(0.03))
    from app.quant.jqengine.engine.jq import api
    assert api._state["ctx"].g.count == 2


def test_strategy_loop_restores_positions(tmp_quant, monkeypatch):
    save_strategy("s2", "s", STRATEGY_NOOP)
    aid = service.account_create("acct2", 100000.0, 0.03, "s2")
    protocol.save_state(aid, {
        "cash": 50000.0, "start_cash": 100000.0, "net_value": 95000.0, "pnl": -5000.0,
        "positions": {"510300.XSHG": {"amount": 5000.0, "avg_cost": 9.0,
                                      "price": 9.0, "today_amount": 0.0}},
        "stop_loss_log": [], "dt": "2026-07-17 09:31",
    })
    _patch_one_loop(monkeypatch)
    runner.run_loop(aid, dm=_StubDM(), feed=_feed_factory(10.0), matcher=Matcher(0.03))
    snaps = db.get_sim_snapshots(aid)
    assert snaps[-1]["positions_value"] == pytest.approx(50000.0)  # 5000 股恢复且按新价估值
    assert snaps[-1]["net_value"] == pytest.approx(100000.0)       # 50000 现金 + 5 万市值
    st = protocol.read_state(aid)
    assert st["positions"]["510300.XSHG"]["avg_cost"] == 9.0
    assert st["positions"]["510300.XSHG"]["price"] == 10.0


def test_strategy_loop_stop_loss_sells(tmp_quant, monkeypatch):
    save_strategy("s3", "s", STRATEGY_NOOP)
    aid = service.account_create("acct3", 100000.0, 0.03, "s3")
    protocol.save_state(aid, {
        "cash": 0.0, "start_cash": 50000.0, "net_value": 50000.0, "pnl": 0.0,
        "positions": {"510300.XSHG": {"amount": 5000.0, "avg_cost": 10.0,
                                      "price": 10.0, "today_amount": 0.0}},
        "stop_loss_log": [], "dt": "2026-07-17 09:31",
    })
    _patch_one_loop(monkeypatch)
    # 不传 matcher：分派器按账户 stop_loss 自建并带 account_id（止损落库依赖它）
    runner.run_loop(aid, dm=_StubDM(), feed=_feed_factory(9.0))
    rows = db.get_sim_stoploss(aid)
    assert len(rows) == 1 and rows[0]["action"] == "STOP_LOSS"
    st = protocol.read_state(aid)
    assert st["positions"] == {}          # 止损清仓回写 portfolio/state
    assert st["cash"] == pytest.approx(5000 * 9.0 * 0.999 * (1 - 0.0003), rel=1e-4)


def test_strategy_loop_missing_strategy_marks_failed(tmp_quant, monkeypatch):
    aid = service.account_create("acct404", 100000.0, 0.03, "no_such_strategy")
    monkeypatch.setattr(runner, "is_paused", lambda aid: False)
    runner.run_loop(aid, dm=_StubDM(), feed=_feed_factory(), matcher=Matcher(0.03))
    assert db.get_sim_account(aid)["status"] == "failed"
    assert any("策略不存在" in l["message"] for l in db.get_sim_logs(aid))


# ---- run_daily 调度 ----
def test_daily_due_times():
    assert runner._daily_due("open", pd.Timestamp("2026-07-17 09:31"))
    assert not runner._daily_due("close", pd.Timestamp("2026-07-17 09:31"))
    assert runner._daily_due("close", pd.Timestamp("2026-07-17 14:59"))
    assert runner._daily_due("10:00", pd.Timestamp("2026-07-17 10:00"))
    assert not runner._daily_due("10:01", pd.Timestamp("2026-07-17 09:31"))


def test_run_daily_fires_once_per_day(tmp_quant, monkeypatch):
    save_strategy("s_once", "s", STRATEGY_BUY)
    aid = service.account_create("acct_once", 100000.0, 0.03, "s_once")
    _patch_one_loop(monkeypatch, ticks=2)
    bars = iter([_today_bar(), _today_bar(minute=31)])

    def _feed(dm, codes, now, acc):
        return {c: 10.0 for c in codes}, next(bars)

    runner.run_loop(aid, dm=_StubDM(), feed=_feed, matcher=Matcher(0.03))
    # 'open' 任务每日只触发一次：两个 bar 也只有一笔买入（第二根 bar 现金已不足再触发目标仓位）
    assert len(db.get_sim_trades(aid)) == 1


# ---- API ----
def test_api_account_strategy_validation(tmp_quant, api_client):
    r = api_client.post("/api/quant/sim/accounts",
                        json={"name": "a", "capital": 10000, "strategy_id": "nope"})
    assert r.status_code == 400
    save_strategy("s_api", "s", STRATEGY_NOOP)
    r = api_client.post("/api/quant/sim/accounts",
                        json={"name": "a", "capital": 10000, "strategy_id": "s_api"})
    assert r.status_code == 200
    assert r.json()["data"]["strategy_id"] == "s_api"


def test_api_sim_logs_endpoint(tmp_quant, api_client):
    db.insert_sim_account("a_log", "a", 1.0, 0.03, "created", "")
    db.insert_sim_log("a_log", "2026-07-18 09:00", "info", "m1")
    db.insert_sim_log("a_log", "2026-07-18 09:01", "warn", "m2")
    r = api_client.get("/api/quant/sim/accounts/a_log/logs")
    assert r.status_code == 200
    rows = r.json()["data"]
    assert [r["message"] for r in rows] == ["m1", "m2"]   # 正序返回
    r = api_client.get("/api/quant/sim/accounts/a_log/logs?limit=1")
    assert [r["message"] for r in r.json()["data"]] == ["m2"]


def test_api_sim_status_includes_strategy_name(tmp_quant, api_client):
    save_strategy("s_nm", "五福轮动", STRATEGY_NOOP)
    db.insert_sim_account("a_nm", "a", 1.0, 0.03, "created", "s_nm")
    r = api_client.get("/api/quant/sim/accounts/a_nm/status")
    assert r.status_code == 200
    assert r.json()["data"]["strategy_name"] == "五福轮动"


# ---- 开始模拟日期：历史补跑 / 未来空转 ----
def test_session_minutes_full_day():
    bars = runner._session_minutes(datetime.date(2026, 7, 17))
    assert len(bars) == 240
    assert bars[0] == datetime.datetime(2026, 7, 17, 9, 31)
    assert bars[-1] == datetime.datetime(2026, 7, 17, 15, 0)
    assert datetime.datetime(2026, 7, 17, 12, 0) not in bars  # 午休无 bar


def _replay_dm_cls(days):
    """带历史日线（交易日判定/取交易日表）与历史分钟价的 stub DM。

    附 set_minute_window / preload_minute_for_pool 桩：记录补跑是否把整个区间钉窗
    口并批量预取池（Task 1 提速的核心行为）。
    """

    class _ReplayDM(_StubDM):
        DAYS = days

        def __init__(self):
            super().__init__()
            self.window_seen = None
            self.pool_seen = None

        def set_minute_window(self, start, end):
            self.window_seen = (str(start)[:10], str(end)[:10])

        def preload_minute_for_pool(self, codes, as_of=None):
            self.pool_seen = list(codes) if codes else []

        def fetch(self, method, *a, **k):
            if method == "get_daily" and a and a[0] == "000300.XSHG":
                return pd.DataFrame({"close": [1.0] * len(self.DAYS)},
                                    index=pd.DatetimeIndex([str(d) for d in self.DAYS]))
            raise RuntimeError("stub: no data")

        def get_minute_price_at(self, code, dt):
            if pd.Timestamp(dt).date() in self.DAYS:
                return 10.0
            return None

    return _ReplayDM


def test_strategy_loop_replays_history_then_live(tmp_quant, monkeypatch):
    today = datetime.date.today()
    days = [today - datetime.timedelta(days=n) for n in (4, 3, 2, 1)]
    save_strategy("s_rp", "s", STRATEGY_BUY)
    aid = service.account_create("acct_rp", 100000.0, 0.03, "s_rp", str(days[0]))
    # 补跑 4 天逐日检查 is_paused，之后实时段只跑 1 个 tick
    _patch_one_loop(monkeypatch, pause_checks_before_loop=len(days))
    runner.run_loop(aid, dm=_replay_dm_cls(days)(), feed=_feed_factory(10.0),
                    matcher=Matcher(0.03))

    trades = db.get_sim_trades(aid)
    # 首日 'open' 买入 5000 股；次日再平衡：手续费侵蚀净值使 50% 目标下移几股，
    # 产生一笔小额 SELL（同时验证了 T+1 跨日解冻——买入次日才可卖）；第三日起
    # 持仓与目标一致，不再成交。
    assert len(trades) == 2
    assert trades[0]["action"] == "BUY" and trades[0]["amount"] == 5000
    assert trades[0]["ts"].startswith(str(days[0]))
    assert trades[1]["action"] == "SELL" and trades[1]["amount"] < 100
    assert trades[1]["ts"].startswith(str(days[1]))
    snaps = db.get_sim_snapshots(aid)
    assert snaps[0]["dt"].startswith(str(days[0]))            # 净值曲线从 start_date 起
    assert snaps[-1]["dt"].startswith(str(today))             # 末行来自实时 tick
    logs = db.get_sim_logs(aid)
    assert any("开始历史补跑" in l["message"] for l in logs)
    assert any("历史补跑完成" in l["message"] for l in logs)
    # 补跑日志时间戳用引擎推进到的时间（bar 时刻/当日），而非真实当前时间
    reb = [l for l in logs if "rebalanced" in l["message"]]
    assert reb and reb[0]["ts"].startswith(str(days[0]))
    start_log = [l for l in logs if "开始历史补跑" in l["message"]]
    done_log = [l for l in logs if "历史补跑完成" in l["message"]]
    assert start_log[0]["ts"].startswith(str(days[0]))
    assert done_log[0]["ts"].startswith(str(days[-1]))


def test_replay_pins_minute_window_and_preloads_pool(tmp_quant, monkeypatch):
    """补跑前必须钉住整个区间分钟窗口并批量预取池，否则滑窗导致池内标的逐日网络回源。"""
    today = datetime.date.today()
    days = [today - datetime.timedelta(days=n) for n in (4, 3, 2, 1)]
    save_strategy("s_pin", "s", STRATEGY_BUY)
    aid = service.account_create("acct_pin", 100000.0, 0.03, "s_pin", str(days[0]))
    _patch_one_loop(monkeypatch, pause_checks_before_loop=len(days))
    dm = _replay_dm_cls(days)()
    runner.run_loop(aid, dm=dm, feed=_feed_factory(10.0), matcher=Matcher(0.03))

    assert dm.window_seen is not None
    assert dm.window_seen[0] == str(days[0])
    # 窗口上界 = 今天（start_date 在今天内，整个补跑区间被钉住）
    assert dm.window_seen[1] == str(today)
    assert dm.pool_seen is not None
    assert "510300.XSHG" in dm.pool_seen           # 策略 universe 标的被批量预取


def test_strategy_loop_replays_today_intraday_bars(tmp_quant, monkeypatch):
    """启动/重置当天（含今日交易日）也要回补开盘到当前时刻，而非只补到昨日。

    回归：_replay_history 原先只补 [start_date, 昨日]，当日盘中/收盘后启动时
    今天日内 bar 全丢（只有 EOD 一条快照）。现在 today 也进补跑范围，只回放到
    bar <= now 的时刻，剩余由实时主循环接管。
    """
    class _FixedNow(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 8, 6, 10, 30, 0)

    monkeypatch.setattr(runner.datetime, "datetime", _FixedNow)
    today = datetime.date(2026, 8, 6)
    days = [datetime.date(2026, 8, 3), datetime.date(2026, 8, 4),
            datetime.date(2026, 8, 5), today]
    save_strategy("s_rpt", "s", STRATEGY_BUY)
    aid = service.account_create("acct_rpt", 100000.0, 0.03, "s_rpt", str(days[0]))
    # 补跑 4 天（含今天）逐日检查 is_paused，之后实时段只跑 1 个 tick
    _patch_one_loop(monkeypatch, pause_checks_before_loop=len(days))
    runner.run_loop(aid, dm=_replay_dm_cls(days)(), feed=_feed_factory(10.0),
                    matcher=Matcher(0.03))

    snaps = db.get_sim_snapshots(aid)
    today_snaps = [s for s in snaps if s["dt"].startswith(str(today))]
    # 今日回补到 10:30（09:31~10:30 共 60 根 bar）；实时 tick 因 last_bar 已推进被去重跳过
    assert len(today_snaps) == 60, len(today_snaps)
    times = sorted(s["dt"][11:16] for s in today_snaps)
    assert "09:31" in times and "10:30" in times
    assert snaps[-1]["dt"].startswith(str(today))
    logs = db.get_sim_logs(aid)
    assert any("今日" in l["message"] and "回补" in l["message"] for l in logs)


STRATEGY_REPLAY_LOG = '''
def init(context):
    context.universe = ["510300.XSHG"]
    g.logged_days = []

def before_trading_start(context):
    log.info("pre-market")

def on_open(context):
    log.info("opened")

run_daily(on_open, "open")
'''


def test_replay_logs_use_engine_forward_time(tmp_quant, monkeypatch):
    """补跑日志时间戳用引擎推进到的时间（bar/当日），而非真实当前时间。"""
    today = datetime.date.today()
    days = [today - datetime.timedelta(days=n) for n in (4, 3, 2, 1)]
    save_strategy("s_rlog", "s", STRATEGY_REPLAY_LOG)
    aid = service.account_create("acct_rlog", 100000.0, 0.03, "s_rlog", str(days[0]))
    _patch_one_loop(monkeypatch, pause_checks_before_loop=len(days))
    runner.run_loop(aid, dm=_replay_dm_cls(days)(), feed=_feed_factory(10.0),
                    matcher=Matcher(0.03))
    logs = db.get_sim_logs(aid)
    opened = [l for l in logs if l["message"] == "opened"]
    pre = [l for l in logs if l["message"] == "pre-market"]
    # 每个补跑日各触发一次，时间戳落在该交易日（引擎推进的时间），而非真实当前时间
    for day in days:
        assert any(l["ts"].startswith(str(day)) for l in opened), (day, opened)
        assert any(l["ts"].startswith(str(day)) for l in pre), (day, pre)


def test_strategy_loop_saved_state_skips_replay(tmp_quant, monkeypatch):
    today = datetime.date.today()
    days = [today - datetime.timedelta(days=n) for n in (4, 3, 2, 1)]
    save_strategy("s_rp2", "s", STRATEGY_NOOP)
    aid = service.account_create("acct_rp2", 100000.0, 0.03, "s_rp2", str(days[0]))
    protocol.save_state(aid, {
        "cash": 100000.0, "start_cash": 100000.0, "net_value": 100000.0, "pnl": 0.0,
        "positions": {}, "stop_loss_log": [], "dt": str(today + datetime.timedelta(days=1)) + " 09:31",
    })
    _patch_one_loop(monkeypatch)
    runner.run_loop(aid, dm=_replay_dm_cls(days)(), feed=_feed_factory(10.0),
                    matcher=Matcher(0.03))
    # 已有存档（续跑）→ 不补跑：只有实时 tick 的一行快照
    snaps = db.get_sim_snapshots(aid)
    assert len(snaps) == 1
    assert snaps[0]["dt"].startswith(str(today))


def test_strategy_loop_future_start_date_idles(tmp_quant, monkeypatch):
    future = str(datetime.date.today() + datetime.timedelta(days=3))
    save_strategy("s_fut", "s", STRATEGY_BUY)
    aid = service.account_create("acct_fut", 100000.0, 0.03, "s_fut", future)
    _patch_one_loop(monkeypatch)
    runner.run_loop(aid, dm=_StubDM(), feed=_feed_factory())
    assert db.get_sim_trades(aid) == []      # 到日前空转，不触发策略
    assert db.get_sim_snapshots(aid) == []


def test_api_account_start_date_validation(tmp_quant, api_client):
    r = api_client.post("/api/quant/sim/accounts",
                        json={"name": "a", "capital": 10000, "start_date": "2026-13-40"})
    assert r.status_code == 400
    r = api_client.post("/api/quant/sim/accounts",
                        json={"name": "a", "capital": 10000, "start_date": "2026-07-01"})
    assert r.status_code == 200
    assert r.json()["data"]["start_date"] == "2026-07-01"


# ---- 运行频率：日频账户每日只驱动一次，且全量触发当日任务 ----
STRATEGY_DAILY = '''
def init(context):
    context.universe = ["510300.XSHG"]
    g.open_n = 0
    g.close_n = 0

def on_open(context):
    g.open_n += 1

def on_close(context):
    g.close_n += 1

run_daily(on_open, "open")
run_daily(on_close, "close")
'''


def test_daily_frequency_one_tick_all_tasks(tmp_quant, monkeypatch):
    save_strategy("s_df", "s", STRATEGY_DAILY)
    aid = service.account_create("acct_df", 100000.0, 0.03, "s_df", "", "daily")
    _patch_one_loop(monkeypatch, ticks=3)
    bars = iter([_today_bar(minute=31), _today_bar(minute=32), _today_bar(minute=33)])

    def _feed(dm, codes, now, acc):
        return {c: 10.0 for c in codes}, next(bars)

    runner.run_loop(aid, dm=_StubDM(), feed=_feed, matcher=Matcher(0.03))
    from app.quant.jqengine.engine.jq import api
    g = api._state["ctx"].g
    assert g.open_n == 1     # 当日唯一 tick 触发一次
    assert g.close_n == 1    # 'close' 任务也在该 tick 全量触发（force_all）
    # 首 bar 处理后 daily_done，后续 bar 不再驱动：快照只有 1 行
    assert len(db.get_sim_snapshots(aid)) == 1


def test_minute_frequency_default_when_unset(tmp_quant):
    aid = service.account_create("acct_md", 100000.0, 0.03)
    assert db.get_sim_account(aid)["frequency"] == "minute"


def test_list_accounts_joins_net_value(tmp_quant):
    aid = service.account_create("acct_lv", 100000.0, 0.03, "", "", "daily")
    protocol.save_state(aid, {
        "cash": 99000.0, "start_cash": 100000.0, "net_value": 99000.0, "pnl": -1000.0,
        "positions": {}, "stop_loss_log": [], "dt": "2026-07-17 09:31",
    })
    aid2 = service.account_create("acct_lv2", 1.0, 0.03)
    rows = {r["id"]: r for r in db.list_sim_accounts()}
    assert rows[aid]["net_value"] == 99000.0
    assert rows[aid]["pnl"] == -1000.0
    assert rows[aid]["frequency"] == "daily"
    assert rows[aid2]["net_value"] is None   # 无状态行的账户为 NULL


def test_api_account_frequency_validation(tmp_quant, api_client):
    r = api_client.post("/api/quant/sim/accounts",
                        json={"name": "a", "capital": 10000, "frequency": "hourly"})
    assert r.status_code == 400
    r = api_client.post("/api/quant/sim/accounts",
                        json={"name": "a", "capital": 10000, "frequency": "daily"})
    assert r.status_code == 200
    assert r.json()["data"]["frequency"] == "daily"
