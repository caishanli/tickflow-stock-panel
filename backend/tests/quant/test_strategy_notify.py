"""策略 log.notify() 端到端集成测试。

验证策略调用 log.notify() 后：
1. sim_logs 中出现 notify level 的日志
2. 账户开启钉钉时 _send_dingtalk_async 被调用
"""
import os
import tempfile
from unittest.mock import patch

from app.quant import db
from app.quant.jqengine.engine.jq.api import log, _state


def _fresh_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    db.init_db(path)
    return path


def test_strategy_notify_writes_log_and_triggers_dingtalk():
    """log.notify() 写入 sim_logs 并触发钉钉异步发送。"""
    p = _fresh_db()
    db.insert_sim_account("a1", "acc", 10000.0, 0.03, "created")
    db.update_sim_account("a1", dingtalk_enabled=1)
    db.set_quant_setting("dingtalk_webhook_url", "https://oapi.dingtalk.com/robot/send?access_token=xxx")

    # 模拟 runner 的 log_sink 注入
    from app.quant.simulate.runner import _emit_log
    _state["log_sink"] = lambda level, msg: _emit_log("a1", level, msg)

    try:
        with patch("app.quant.simulate.runner._send_dingtalk_async") as mock_send:
            log.notify("买入159985, 数量4600, 价格2.132")
            # 验证 sim_logs 中有 notify level
            logs = db.get_sim_logs("a1")
            levels = [l["level"] for l in logs]
            assert "info" in levels
            assert "notify" in levels
            # 验证钉钉被调用
            assert mock_send.called
            call_args = mock_send.call_args[0]
            assert call_args[0] == "a1"
            assert "买入159985" in call_args[1]
    finally:
        _state.pop("log_sink", None)
        os.unlink(p)


def test_strategy_notify_dingtalk_disabled():
    """账户未开启钉钉时，log.notify() 仍写日志但不触发发送。"""
    p = _fresh_db()
    db.insert_sim_account("a2", "acc2", 10000.0, 0.03, "created")

    from app.quant.simulate.runner import _emit_log
    _state["log_sink"] = lambda level, msg: _emit_log("a2", level, msg)

    try:
        with patch("app.quant.simulate.runner._send_dingtalk_async") as mock_send:
            log.notify("测试消息")
            logs = db.get_sim_logs("a2")
            assert any(l["level"] == "notify" for l in logs)
            assert not mock_send.called
    finally:
        _state.pop("log_sink", None)
        os.unlink(p)
