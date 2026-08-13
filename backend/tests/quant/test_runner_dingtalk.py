"""Runner 钉钉推送集成测试（mock send_dingtalk，不联网）。"""
import datetime
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from app.quant import db
from app.quant.simulate.runner import _emit_log


def _fresh_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    db.init_db(path)
    return path


def test_notify_level_triggers_dingtalk_when_enabled():
    """账户开启钉钉 + 配了 webhook 时，notify level 触发异步发送。"""
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    db.set_quant_setting("dingtalk_webhook_url", "https://oapi.dingtalk.com/robot/send?access_token=xxx")
    db.set_quant_setting("dingtalk_secret", "")
    try:
        with patch("app.quant.simulate.runner._send_dingtalk_async") as mock_send:
            _emit_log("a1", "notify", "买入159985")
            assert mock_send.called
    finally:
        os.unlink(p)


def test_notify_level_skipped_when_disabled():
    """账户未开启钉钉时，notify level 不触发发送。"""
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    try:
        with patch("app.quant.simulate.runner._send_dingtalk_async") as mock_send:
            _emit_log("a1", "notify", "买入159985")
            assert not mock_send.called
    finally:
        os.unlink(p)


def test_info_level_never_triggers_dingtalk():
    """普通 info level 不触发钉钉。"""
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    db.set_quant_setting("dingtalk_webhook_url", "https://oapi.dingtalk.com/robot/send?access_token=xxx")
    try:
        with patch("app.quant.simulate.runner._send_dingtalk_async") as mock_send:
            _emit_log("a1", "info", "普通日志")
            assert not mock_send.called
    finally:
        os.unlink(p)


from app.quant.simulate.runner import _build_stop_loss_notify, _dispatch_dingtalk


class _SyncExecutor:
    """同步 executor 桩：submit 立即执行，消除异步竞态。"""

    def submit(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


def test_build_stop_loss_notify_format():
    rec = {"name": "标普油气ETF嘉实", "code": "159518.XSHE", "price": 1.163,
           "amount": 88000.0, "pnl": -2992.0, "pnl_pct": -0.0312,
           "commission": 10.23}
    msg = _build_stop_loss_notify(0.03, rec)
    assert "🚨 【账户止损】标普油气ETF嘉实(159518.XSHE)" in msg
    assert "-3%止损" in msg
    assert "卖出88000份" in msg
    assert "价格1.163" in msg
    assert "佣金10.23" in msg
    assert "盈亏-2992.00(-3.12%)" in msg


def test_dispatch_forwards_engine_ts():
    """补跑通知落款用引擎推进的 bar 时间，透传到 _send_dingtalk_async。"""
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    try:
        with patch("app.quant.simulate.runner._DINGTALK_EXECUTOR", _SyncExecutor()), \
             patch("app.quant.simulate.runner._send_dingtalk_async") as mock_send:
            _dispatch_dingtalk("a1", "📥 买入 测试", ts="2026-07-10 13:10:00")
            mock_send.assert_called_once_with("a1", "📥 买入 测试", "2026-07-10 13:10:00")
    finally:
        os.unlink(p)


def test_dispatch_without_ts_keeps_two_args():
    """实时模式不传 ts，维持原两参数调用形态。"""
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    try:
        with patch("app.quant.simulate.runner._DINGTALK_EXECUTOR", _SyncExecutor()), \
             patch("app.quant.simulate.runner._send_dingtalk_async") as mock_send:
            _dispatch_dingtalk("a1", "📤 卖出 测试")
            mock_send.assert_called_once_with("a1", "📤 卖出 测试", None)
    finally:
        os.unlink(p)


def test_emit_log_replay_accumulates_not_dispatch():
    """补跑期间 notify 累积到 _replay_day_notifies，不逐笔推送。"""
    from app.quant.simulate import runner
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    try:
        runner._replay_day_notifies.clear()
        runner._replay_active_ids.add("a1")
        try:
            with patch("app.quant.simulate.runner._dispatch_dingtalk") as mock_d:
                runner._emit_log("a1", "notify", "📥 买入 测试(159985.XSHE) 数量100 价格2.1 佣金0.02",
                                 ts="2026-07-21 13:10:00")
                assert not mock_d.called
                assert runner._replay_day_notifies == [("13:10", "📥 买入 测试(159985.XSHE) 数量100 价格2.1 佣金0.02")]
        finally:
            runner._replay_active_ids.discard("a1")
            runner._replay_day_notifies.clear()
    finally:
        os.unlink(p)


def test_emit_log_live_dispatches():
    """实时（非补跑）notify 逐笔推送。"""
    from app.quant.simulate import runner
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    try:
        runner._replay_active_ids.discard("a1")
        runner._replay_day_notifies.clear()
        with patch("app.quant.simulate.runner._dispatch_dingtalk") as mock_d:
            runner._emit_log("a1", "notify", "📥 买入 测试")
            mock_d.assert_called_once()
    finally:
        runner._replay_day_notifies.clear()
        os.unlink(p)


def test_build_daily_summary_table():
    """当日汇总：买卖/止损解析成表格行 + 收益脚注。"""
    from app.quant.simulate.runner import _build_daily_summary
    notifies = [
        ("09:41", "📤 卖出 标普油气ETF嘉实(159518.XSHE) 数量88000 价格1.163 佣金10.23 盈利+2992.00(+3.01%) 持仓2个交易日"),
        ("13:10", "📥 买入 南方原油(501018.XSHG) 数量51400 价格1.989 佣金10.22"),
        ("14:52", "🚨 【账户止损】豆粕ETF华夏(159985.XSHE) 触发-3%止损 卖出46400份 价格2.143 佣金9.94 盈亏-3301.13(-3.12%)"),
    ]
    msg = _build_daily_summary("2026-07-21", notifies, 103912.31, 1234.5, 0.012, 3912.31, 0.039, 1)
    assert "### 📊 模拟盘回放 2026-07-21" in msg
    assert "| 时间 | 方向 | 标的 | 数量 | 价格 | 盈亏 |" in msg
    assert "| 09:41 | 卖出 | 标普油气ETF嘉实(159518.XSHE) | 88000 | 1.163 | +2992.00(+3.01%) |" in msg
    assert "| 13:10 | 买入 | 南方原油(501018.XSHG) | 51400 | 1.989 | — |" in msg
    assert "| 14:52 | 止损 | 豆粕ETF华夏(159985.XSHE) | 46400 | 2.143 | -3301.13(-3.12%) |" in msg
    assert "📈 当日 +1,234.50 (+1.20%)" in msg


def test_build_daily_summary_idle():
    """无换仓日汇总：只有无换仓行。"""
    from app.quant.simulate.runner import _build_daily_summary
    msg = _build_daily_summary("2026-07-20",
                               [("13:10", "🈳 今日无换仓：持有1只，维持当前仓位")],
                               100000.0, 0.0, 0.0, 0.0, 0.0, 1)
    assert "| 13:10 | 无换仓 | 维持当前仓位 | — | — | — |" in msg


def test_build_daily_pnl_format():
    """实时每日收盘收益消息格式。"""
    from app.quant.simulate.runner import _build_daily_pnl
    msg = _build_daily_pnl("2026-08-13", 105432.10, 1234.56, 0.0123, 5432.10, 0.0543, 1)
    assert "### 📈 模拟盘日收益 2026-08-13" in msg
    assert "当日收益: +1,234.56 (+1.23%)" in msg
    assert "累计收益: +5,432.10 (+5.43%)" in msg
    assert "总资产: 105,432.10 | 持仓: 1只" in msg


def test_emit_eod_notify_replay_summary():
    """补跑收盘推送当日汇总表格：取用当日累积、清空、推进 prev_close_net。"""
    from app.quant.simulate import runner
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 100000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    try:
        runner._replay_day_notifies.clear()
        runner._replay_day_notifies.append(("13:10", "📥 买入 测试(159985.XSHE) 数量100 价格2.1 佣金0.02"))
        ctx = SimpleNamespace(portfolio=SimpleNamespace(positions={}))
        state = {"net_value": 101000.0, "pnl": 1000.0}
        aux = {"start_cash": 100000.0, "replay_mode": True}
        now = datetime.datetime(2026, 7, 21, 15, 5)
        with patch("app.quant.simulate.runner._dispatch_dingtalk") as mock_d:
            runner._emit_eod_notify("a1", ctx, state, aux, now)
        mock_d.assert_called_once()
        msg = mock_d.call_args[0][1]
        assert "### 📊 模拟盘回放 2026-07-21" in msg
        assert "📈 当日 +1,000.00 (+1.00%)" in msg
        assert runner._replay_day_notifies == []
        assert aux["prev_close_net"] == 101000.0
    finally:
        runner._replay_day_notifies.clear()
        os.unlink(p)
