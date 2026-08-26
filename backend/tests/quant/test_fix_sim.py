"""M1/M2/M3/M4/M5/M12/M15/SSE 终态退出/frequency 透传修复的回归测试。

全部为离线测试：子进程（Popen/killpg）与数据源均 patch 掉，不派生真实 worker。
"""
from __future__ import annotations

import datetime
import json

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.quant import db
from app.quant import service
from app.quant.config import CONFIG
from app.quant.simulate import protocol
from app.quant.simulate import runner
from app.quant.simulate.matcher import Matcher


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    runtime = tmp_path / "quant_sim"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(runtime))
    db.init_db(str(db_path))
    return tmp_path


@pytest.fixture
def api_client(tmp_quant):
    from app.quant.api.quant import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class _FakeProvider:
    """离线数据源：frames 提供分钟线，daily 提供日线（跌停判定用）。"""

    def __init__(self, frames=None, daily=None):
        self.frames = frames or {}
        self.daily = daily

    def get_minute(self, code, date):
        return self.frames.get(code)

    def get_daily(self, code, start, end):
        return self.daily


def _mk_state(**over):
    state = {
        "cash": 50000.0, "start_cash": 100000.0, "net_value": 100000.0, "pnl": 0.0,
        "positions": {"600000.XSHG": {"amount": 5000.0, "avg_cost": 10.0, "price": 10.0}},
        "stop_loss_log": [], "dt": "2026-07-17 10:00:00",
    }
    state.update(over)
    return state


# ---- M1：save→read 往返持仓不丢 ----
def test_m1_read_sim_state_parses_positions(tmp_quant):
    db.insert_sim_account("a_m1", "a", 100000.0, 0.03, "created")
    db.upsert_sim_state("a_m1", 99000.0, '{"600000.XSHG":{"amount":100.0}}',
                        99000.0, -1000.0, 100000.0, '[{"code":"X"}]', "2026-07-17 10:00")
    st = db.read_sim_state("a_m1")
    assert st["positions"] == {"600000.XSHG": {"amount": 100.0}}
    assert st["stop_loss_log"] == [{"code": "X"}]


def test_m1_read_sim_state_bad_json_falls_back(tmp_quant):
    db.insert_sim_account("a_bad", "a", 1.0, 0.03, "created")
    db.upsert_sim_state("a_bad", 1.0, "{oops", 1.0, 0.0, 1.0, "not-json", "d")
    st = db.read_sim_state("a_bad")
    assert st["positions"] == {} and st["stop_loss_log"] == []
    # 无状态行的账户默认持仓为空 dict
    assert db.read_sim_state("a_none")["positions"] == {}


def test_m1_read_sim_state_no_row_is_null_not_zero(tmp_quant):
    """无状态行（如重置后）的 net_value/start_cash 应为 None 而非伪造 0.0。

    回归：重置账户后前端"收益率"变 -100%——read_sim_state 对无状态行返回
    net_value=0.0/start_cash=0.0，前端 baseNV 兜底成 1 → 0/1-1 = -1 显示 -100.00%。
    """
    st = db.read_sim_state("a_none2")
    assert st["net_value"] is None
    assert st["start_cash"] is None
    assert st["cash"] is None
    assert st["pnl"] is None
    assert st["positions"] == {}


def test_m1_protocol_roundtrip_keeps_positions(tmp_quant):
    db.insert_sim_account("a_rt", "a", 100000.0, 0.03, "created")
    state = _mk_state()
    protocol.save_state("a_rt", state)
    back = protocol.read_state("a_rt")
    assert back["positions"]["600000.XSHG"]["amount"] == 5000.0
    assert back["positions"]["600000.XSHG"]["avg_cost"] == 10.0
    assert back["cash"] == 50000.0


# ---- M2：runner 健壮性 ----
def test_m2_in_trading_uses_now_arg():
    sat = datetime.datetime(2026, 7, 18, 10, 0)   # 周六
    mon = datetime.datetime(2026, 7, 20, 10, 0)   # 周一
    assert runner.in_trading(sat) is False
    assert runner.in_trading(mon) is True
    assert runner.in_trading(datetime.datetime(2026, 7, 20, 12, 0)) is False
    assert runner.in_trading(datetime.datetime(2026, 7, 20, 14, 0)) is True


# ---- M2：11:30 bar 实盘宽限（TICK_OFFSET=8 时 in_trading 已截止）----
def test_m2_tick_window_covers_morning_end_grace():
    # 2026-07-20 周一
    assert runner._tick_window(datetime.datetime(2026, 7, 20, 11, 29, 30)) is True   # 盘中
    assert runner._tick_window(datetime.datetime(2026, 7, 20, 11, 30, 8)) is True    # 11:30 bar 实盘处理窗口
    assert runner._tick_window(datetime.datetime(2026, 7, 20, 11, 30, 58)) is True
    assert runner._tick_window(datetime.datetime(2026, 7, 20, 11, 31, 8)) is False   # 午休
    assert runner._tick_window(datetime.datetime(2026, 7, 20, 12, 0, 0)) is False
    assert runner._tick_window(datetime.datetime(2026, 7, 20, 15, 1, 0)) is True     # 下午宽限
    assert runner._tick_window(datetime.datetime(2026, 7, 20, 15, 3, 0)) is False


def _patch_one_loop(monkeypatch):
    """让 run_loop 只跑一轮交易时段巡检后退出。"""
    pauses = iter([False, True])
    monkeypatch.setattr(runner, "is_paused", lambda aid: next(pauses))
    monkeypatch.setattr(runner, "in_trading", lambda now=None: True)
    sleeps = []
    monkeypatch.setattr(runner.time, "sleep", lambda s: sleeps.append(s))
    return sleeps


def test_m2_missing_minute_data_skips_without_crash(tmp_quant, monkeypatch):
    db.insert_sim_account("a_m2", "a", 100000.0, 0.03, "running")
    protocol.save_state("a_m2", _mk_state())
    sleeps = _patch_one_loop(monkeypatch)
    provider = _FakeProvider()  # get_minute 返回 None（停牌/节假日/数据源抖动）
    runner.run_loop("a_m2", provider=provider, matcher=Matcher(0.03), poll_interval=60)
    st = protocol.read_state("a_m2")
    assert "600000.XSHG" in st["positions"]          # 不崩、不丢仓、不止损
    assert sleeps == [60]                            # 交易时段分支有 sleep 限速
    assert db.get_sim_account("a_m2")["status"] == "paused"


def test_m2_minute_data_raises_skips_without_crash(tmp_quant, monkeypatch):
    class _BoomProvider(_FakeProvider):
        def get_minute(self, code, date):
            raise RuntimeError("数据源异常")

    db.insert_sim_account("a_m2b", "a", 100000.0, 0.03, "running")
    protocol.save_state("a_m2b", _mk_state())
    _patch_one_loop(monkeypatch)
    runner.run_loop("a_m2b", provider=_BoomProvider(), matcher=Matcher(0.03))
    assert "600000.XSHG" in protocol.read_state("a_m2b")["positions"]


def test_m2_crash_marks_account_failed(tmp_quant, monkeypatch):
    class _BoomMatcher:
        def step(self, state, prices, **kw):
            raise RuntimeError("boom")

    db.insert_sim_account("a_m2c", "a", 100000.0, 0.03, "running")
    protocol.save_state("a_m2c", _mk_state())
    monkeypatch.setattr(runner, "is_paused", lambda aid: False)
    monkeypatch.setattr(runner, "in_trading", lambda now=None: True)
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)
    provider = _FakeProvider({"600000.XSHG": pd.DataFrame({"close": [10.0]})})
    with pytest.raises(RuntimeError):
        runner.run_loop("a_m2c", provider=provider, matcher=_BoomMatcher())
    assert db.get_sim_account("a_m2c")["status"] == "failed"


# ---- M15：快照真实持仓市值 + 止损落库 ----
def test_m15_snapshot_has_real_positions_value(tmp_quant, monkeypatch):
    db.insert_sim_account("a_m15", "a", 100000.0, 0.03, "running")
    protocol.save_state("a_m15", _mk_state())
    _patch_one_loop(monkeypatch)
    minute = pd.DataFrame({"close": [10.0]})
    provider = _FakeProvider({"600000.XSHG": minute})
    runner.run_loop("a_m15", provider=provider, matcher=Matcher(0.03))
    snaps = db.get_sim_snapshots("a_m15")
    assert len(snaps) == 1
    assert snaps[0]["positions_value"] == 50000.0    # 5000 股 * 10.0，不再恒 0
    assert snaps[0]["net_value"] == 100000.0


def test_m15_stoploss_persisted_to_db(tmp_quant):
    m = Matcher(0.03, account_id="a_sl")
    state = _mk_state(cash=0.0)
    m.step(state, {"600000.XSHG": 9.0})
    rows = db.get_sim_stoploss("a_sl")
    assert len(rows) == 1
    assert rows[0]["code"] == "600000.XSHG"
    assert rows[0]["action"] == "STOP_LOSS"
    assert rows[0]["pnl_pct"] == pytest.approx(-0.1, abs=1e-4)


# ---- M3：撮合费用/规则 ----
def test_m3_stock_sell_fee_stamp_tax_and_slippage(tmp_quant):
    m = Matcher(0.03)
    state = _mk_state(cash=0.0)
    out = m.step(state, {"600000.XSHG": 9.0},
                 fee=0.0003, stamp_tax=0.0005, slippage=0.001)
    expected = 5000.0 * 9.0 * (1 - 0.001) * (1 - 0.0003 - 0.0005)
    assert out["cash"] == pytest.approx(expected, abs=1e-2)
    assert "600000.XSHG" not in out["positions"]


def test_m3_etf_exempt_stamp_tax(tmp_quant):
    m = Matcher(0.03)
    state = _mk_state(cash=0.0, positions={
        "510300.XSHG": {"amount": 5000.0, "avg_cost": 10.0, "price": 10.0}})
    out = m.step(state, {"510300.XSHG": 9.0},
                 fee=0.0003, stamp_tax=0.0005, slippage=0.001)
    expected = 5000.0 * 9.0 * (1 - 0.001) * (1 - 0.0003)  # ETF 免印花税
    assert out["cash"] == pytest.approx(expected, abs=1e-2)


def test_m3_t1_same_day_buy_not_sellable(tmp_quant):
    m = Matcher(0.03)
    state = _mk_state(cash=0.0, positions={"600000.XSHG": {
        "amount": 5000.0, "avg_cost": 10.0, "price": 10.0, "buy_dt": "2026-07-17"}})
    out = m.step(state, {"600000.XSHG": 9.0})
    assert "600000.XSHG" in out["positions"]   # 当日买入不可卖，止损顺延
    assert out["stop_loss_log"] == []


def test_m3_t1_partial_today_amount(tmp_quant):
    m = Matcher(0.03)
    state = _mk_state(cash=0.0, positions={"600000.XSHG": {
        "amount": 5000.0, "avg_cost": 10.0, "price": 10.0, "today_amount": 2000.0}})
    out = m.step(state, {"600000.XSHG": 9.0}, fee=0.0, stamp_tax=0.0, slippage=0.0)
    assert out["positions"]["600000.XSHG"]["amount"] == 2000.0  # 仅卖出可卖部分
    assert out["cash"] == pytest.approx(3000.0 * 9.0, abs=1e-2)


def test_m3_no_sell_blocks_limit_down(tmp_quant):
    m = Matcher(0.03)
    state = _mk_state(cash=0.0)
    out = m.step(state, {"600000.XSHG": 9.0}, no_sell={"600000.XSHG"})
    assert "600000.XSHG" in out["positions"]   # 跌停/停牌禁止卖出
    assert out["stop_loss_log"] == []


def test_m3_runner_marks_limit_down_no_sell(tmp_quant, monkeypatch):
    db.insert_sim_account("a_ld", "a", 100000.0, 0.03, "running")
    protocol.save_state("a_ld", _mk_state(cash=0.0))
    _patch_one_loop(monkeypatch)
    minute = pd.DataFrame({"close": [9.0]})  # 昨收 10.0 → 跌 10% 触发跌停禁卖
    # runner 内部用真实系统日期，日线日期按当天相对构造
    today = datetime.date.today()
    daily = pd.DataFrame({
        "date": [str(today - datetime.timedelta(days=1)), str(today)],
        "close": [10.0, 9.0],
    })
    provider = _FakeProvider({"600000.XSHG": minute}, daily=daily)
    runner.run_loop("a_ld", provider=provider, matcher=Matcher(0.03))
    st = protocol.read_state("a_ld")
    assert "600000.XSHG" in st["positions"]    # 跌停止损顺延
    assert db.get_sim_stoploss("a_ld") == []


# ---- M4：账户生命周期并发 ----
class _FakeProc:
    pid = 43210


def test_m4_account_start_idempotent(tmp_quant, monkeypatch):
    aid = service.account_create("acct_m4", 100000.0, 0.03)
    calls = []
    monkeypatch.setattr(service.subprocess, "Popen",
                        lambda *a, **k: calls.append((a, k)) or _FakeProc())
    service.account_start(aid)
    service.account_start(aid)  # running 状态重复 start 不再拉起进程
    assert len(calls) == 1
    assert db.get_sim_account(aid)["pid"] == 43210


def test_m4_account_reset_kills_running_process(tmp_quant, monkeypatch):
    aid = service.account_create("acct_m4b", 100000.0, 0.03)
    db.update_sim_account(aid, status="running", pid=54321)
    killed = []
    monkeypatch.setattr(service.os, "killpg", lambda pid, sig: killed.append(pid))
    service.account_reset(aid)
    assert killed == [54321]
    row = db.get_sim_account(aid)
    assert row["status"] == "created" and row["pid"] is None


def test_m4_reset_without_pid_falls_back_to_pause_file(tmp_quant):
    aid = service.account_create("acct_m4c", 100000.0, 0.03)
    db.update_sim_account(aid, status="running")  # 旧进程无 pid
    service.account_reset(aid)
    import os
    assert os.path.exists(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"))


def test_m5_kill_process_group_ignores_dead_pid(tmp_quant):
    import subprocess as sp
    p = sp.Popen(["true"], start_new_session=True)
    p.wait()
    assert service.kill_process_group(p.pid) is False  # 已退出不抛异常
    assert service.kill_process_group(None) is False
    assert service.kill_process_group(0) is False


# ---- M5：Popen 落 pid / 失败置 failed / terminate 杀进程组 ----
def test_m5_submit_backtest_records_pid(tmp_quant, monkeypatch):
    monkeypatch.setattr(service.subprocess, "Popen", lambda *a, **k: _FakeProc())
    run_id = service.submit_backtest({"strategy_id": "", "frequency": "minute"})
    row = db.get_run(run_id)
    assert row["pid"] == 43210
    # #9：frequency 透传进 params_json（桥接侧消费）
    assert json.loads(row["params_json"])["frequency"] == "minute"


def test_m5_submit_backtest_popen_failure_marks_failed(tmp_quant, monkeypatch):
    def _boom(*a, **k):
        raise OSError("no exec")

    monkeypatch.setattr(service.subprocess, "Popen", _boom)
    with pytest.raises(OSError):
        service.submit_backtest({"strategy_id": ""})
    run = db.list_runs(1)[0]
    assert run["status"] == "failed"
    assert "spawn failed" in (run["error"] or "")


def test_m5_terminate_kills_process_group(tmp_quant, api_client, monkeypatch):
    db.insert_run("run_t", "s", "", "{}", "running")
    db.set_run_pid("run_t", 65432)
    killed = []
    monkeypatch.setattr(service.os, "killpg", lambda pid, sig: killed.append(pid))
    r = api_client.post("/api/quant/backtest/run_t/terminate")
    assert r.status_code == 200
    assert killed == [65432]
    row = db.get_run("run_t")
    assert row["status"] == "failed" and row["error"] == "terminated"


def test_m5_terminate_missing_run_404(tmp_quant, api_client):
    assert api_client.post("/api/quant/backtest/nope/terminate").status_code == 404


# ---- sim_equity：沪深300 基准需识别 trade_dt 日期列（否则基线平直为 0）----
def test_sim_equity_benchmark_uses_trade_dt_column(tmp_quant, api_client, monkeypatch):
    from app.quant.jqengine.datasource import manager as dm_mgr

    class _BenchDM:
        def fetch(self, method, *a, **k):
            return pd.DataFrame({
                "close": [100.0, 101.0, 103.0],
                "trade_dt": ["2026-07-09", "2026-07-10", "2026-07-13"],
            })

    monkeypatch.setattr(dm_mgr, "get_data_manager", lambda *a, **k: _BenchDM())
    db.insert_sim_account("a_bm", "a", 100000.0, 0.03, "created")
    db.insert_sim_snapshot("a_bm", "2026-07-10 09:31:00", 100000.0, 100000.0, 0.0, 0.0, 0.0)
    db.insert_sim_snapshot("a_bm", "2026-07-13 09:31:00", 101000.0, 100000.0, 1000.0, 1000.0, 0.01)
    r = api_client.get("/api/quant/sim/accounts/a_bm/equity")
    assert r.status_code == 200
    bm = {s["dt"][:10]: s.get("benchmark_pct") for s in r.json()["data"]}
    # 基准日 = 启动日前一交易日（07-09 收盘 100）→ 07-10 = +1.0%，07-13 = +3.0%
    assert bm["2026-07-10"] == pytest.approx(1.0)
    assert bm["2026-07-13"] == pytest.approx(3.0)


def test_sim_equity_benchmark_falls_back_to_datetime_index(tmp_quant, api_client, monkeypatch):
    from app.quant.jqengine.datasource import manager as dm_mgr

    class _IndexDM:
        def fetch(self, method, *a, **k):
            return pd.DataFrame({"close": [100.0, 101.0]},
                                index=pd.DatetimeIndex(["2026-07-09", "2026-07-10"]))

    monkeypatch.setattr(dm_mgr, "get_data_manager", lambda *a, **k: _IndexDM())
    db.insert_sim_account("a_bmi", "a", 100000.0, 0.03, "created")
    db.insert_sim_snapshot("a_bmi", "2026-07-10 09:31:00", 100000.0, 100000.0, 0.0, 0.0, 0.0)
    r = api_client.get("/api/quant/sim/accounts/a_bmi/equity")
    bm = {s["dt"][:10]: s.get("benchmark_pct") for s in r.json()["data"]}
    assert bm["2026-07-10"] == pytest.approx(1.0)


# ---- 补跑期间状态卡片不更新：status 事件只在状态切换时推送 ----
async def test_sim_stream_emits_status_when_state_changes(tmp_quant, monkeypatch):
    """补跑时 account.status 恒为 running，但 sim_state 每 bar 更新。

    回归：卡片（净值/收益率/持仓）只消费 status 事件里的 state，而 status 事件
    仅在状态切换时推送 → 补跑期间卡片不更新。修复后 state 变化也要推 status 事件。
    """
    from app.quant.api import quant as quant_api
    import asyncio

    async def _no_sleep(*a, **k):
        return None

    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    db.insert_sim_account("a_stchg", "s", 100000.0, 0.03, "running")
    db.upsert_sim_state("a_stchg", 100000.0, "{}", 100000.0, 0.0, 100000.0, "[]",
                        "2026-07-10 09:31:00")
    resp = quant_api.sim_stream("a_stchg")
    agen = resp.body_iterator  # type: ignore[attr-defined]
    # 第一轮：状态切换（None → running）推一条 status
    first = await anext(agen)
    assert "event: status" in first
    assert '"status": "running"' in first
    # 状态不变（仍 running），但 sim_state 更新（净额变化）→ 必须再推一条 status
    db.upsert_sim_state("a_stchg", 99000.0, "{}", 101000.0, 1000.0, 100000.0, "[]",
                        "2026-07-10 09:32:00")
    second = await anext(agen)
    assert "event: status" in second
    assert "101000.0" in second
    await agen.aclose()


async def test_sim_stream_trade_event_includes_name(tmp_quant):
    """SSE trade 事件透传 sim_trades.name。"""
    from app.quant.api import quant as quant_api
    import asyncio

    db.insert_sim_account("a_nm", "s", 100000.0, 0.03, "running")
    db.upsert_sim_state("a_nm", 100000.0, "{}", 100000.0, 0.0, 100000.0, "[]",
                        "2024-01-02 09:31:00")
    resp = quant_api.sim_stream("a_nm")
    agen = resp.body_iterator  # type: ignore[attr-defined]
    # 首轮 status 事件已推（off_trade 定格为 0）后再插入成交 → 该行作为
    # rowid 增量被 get_sim_trades_after 推成 SSE trade 事件
    first = await asyncio.wait_for(anext(agen), timeout=1.0)
    assert "event: status" in first
    db.insert_sim_trade("a_nm", "2024-01-02 09:31", "159985.XSHE", "BUY",
                        2.139, 100, 0.0, 0.0, 9.99, "豆粕ETF华夏")
    second = await asyncio.wait_for(anext(agen), timeout=1.0)
    assert "event: trade" in second
    assert '"name": "豆粕ETF华夏"' in second
    await agen.aclose()


# ---- M12 + #8：SSE 断点续推与终态关流 ----
def test_m12_sse_since_id_resumes_from_snapshot(tmp_quant, api_client):
    db.insert_run("run_s", "s", "", "{}", "done")
    for i in range(3):
        db.insert_equity_row("run_s", f"2024-01-0{i + 2}", 1.0 + i, 1.0, 0.5, 0.5)
    ids = [r["rowid"] for r in db.get_equity_after("run_s", 0)]
    r = api_client.get(f"/api/quant/backtest/run_s/stream?since_id={ids[1]}")
    assert r.status_code == 200
    assert r.text.count("event: equity") == 1   # 只推 rowid > since_id 的一行
    assert '"2024-01-04"' in r.text
    r2 = api_client.get(f"/api/quant/backtest/run_s/stream?last_id={ids[1]}")
    assert r2.text.count("event: equity") == 1  # last_id 别名等价


def test_8_sse_terminal_run_closes_after_increment(tmp_quant, api_client):
    db.insert_run("run_d", "s", "", "{}", "done")
    db.insert_equity_row("run_d", "2024-01-02", 1.0, 1.0, 0.5, 0.5)
    r = api_client.get("/api/quant/backtest/run_d/stream")  # 不传 since_id 保持现行为
    assert r.status_code == 200               # 终态 run 正常关流（请求能返回即未死循环）
    assert "event: status" in r.text
    assert "event: equity" not in r.text      # 现行为：只推建连后的新行


# ---- #9：API 侧 frequency 透传 ----
def test_9_backtest_run_passes_frequency(tmp_quant, api_client, monkeypatch):
    # test_api_quant 的 fixture 先 patch service.submit_backtest 再首次导入 api.quant，
    # 使 api.quant 内的 submit_backtest 永久绑定成测试替身；这里显式绑回真实函数
    from app.quant.api import quant as quant_api
    monkeypatch.setattr(quant_api, "submit_backtest", service.submit_backtest)
    monkeypatch.setattr(service.subprocess, "Popen", lambda *a, **k: _FakeProc())
    r = api_client.post("/api/quant/backtest/run", json={
        "strategy_code": "x", "symbols": ["600000.XSHG"],
        "start": "2024-01-01", "end": "2024-02-01", "frequency": "minute"})
    assert r.status_code == 200
    run_id = r.json()["data"]["run_id"]
    assert json.loads(db.get_run(run_id)["params_json"])["frequency"] == "minute"
    # 缺省为 daily
    r2 = api_client.post("/api/quant/backtest/run", json={
        "strategy_code": "x", "symbols": ["600000.XSHG"],
        "start": "2024-01-01", "end": "2024-02-01"})
    run_id2 = r2.json()["data"]["run_id"]
    assert json.loads(db.get_run(run_id2)["params_json"])["frequency"] == "daily"
