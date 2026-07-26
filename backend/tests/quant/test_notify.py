"""钉钉通知发送单元测试（mock requests.post，不联网）。"""

from unittest.mock import MagicMock, patch

import requests

from app.quant import notify

WEBHOOK_URL = "https://oapi.dingtalk.com/robot/send?access_token=abc123"


def test_send_dingtalk_plain_text():
    """无加签：发送 Markdown，验证 URL 不变 + payload 格式 + 5s 超时。"""
    resp = MagicMock()
    resp.json.return_value = {"errcode": 0, "errmsg": "ok"}
    with patch("app.quant.notify.requests.post", return_value=resp) as mock_post:
        ok = notify.send_dingtalk(WEBHOOK_URL, "", "Test", "Hello")

    assert ok is True
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == WEBHOOK_URL
    assert kwargs["json"] == {
        "msgtype": "markdown",
        "markdown": {"title": "Test", "text": "Hello"},
    }
    assert kwargs["timeout"] == 5


def test_send_dingtalk_with_sign():
    """带加签：URL 应追加 timestamp + sign 参数。"""
    resp = MagicMock()
    resp.json.return_value = {"errcode": 0, "errmsg": "ok"}
    with patch("app.quant.notify.requests.post", return_value=resp) as mock_post:
        ok = notify.send_dingtalk(WEBHOOK_URL, "SECtest123", "Test", "Hello")

    assert ok is True
    args, _kwargs = mock_post.call_args
    url = args[0]
    assert url.startswith(WEBHOOK_URL + "&")
    assert "timestamp=" in url
    assert "sign=" in url


def test_send_dingtalk_failure():
    """钉钉返回 errcode != 0 时返回 False。"""
    resp = MagicMock()
    resp.json.return_value = {"errcode": 310000, "errmsg": "invalid token"}
    with patch("app.quant.notify.requests.post", return_value=resp):
        ok = notify.send_dingtalk(WEBHOOK_URL, "", "Test", "Hello")

    assert ok is False


def test_send_dingtalk_network_error():
    """网络异常时不抛异常，返回 False（fire-and-forget）。"""
    with patch(
        "app.quant.notify.requests.post",
        side_effect=requests.RequestException("timeout"),
    ):
        ok = notify.send_dingtalk(WEBHOOK_URL, "", "Test", "Hello")

    assert ok is False
