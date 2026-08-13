"""Runner 钉钉推送集成测试（mock send_dingtalk，不联网）。"""
import os
import tempfile
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


from app.quant.simulate.runner import _build_stop_loss_notify


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
