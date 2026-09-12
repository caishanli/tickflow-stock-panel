"""钉钉自定义机器人消息发送（Markdown + 加签，fire-and-forget）。

发送失败永不抛异常：调用方只需关心 True/False，适合模拟盘定时推送场景。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Any

import requests

log = logging.getLogger(__name__)

_TIMEOUT = 5


def _sign(secret: str, timestamp: int) -> str:
    """钉钉加签：HMAC-SHA256 over f"{timestamp}\\n{secret}"，base64 + urlencode。"""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(
        secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return urllib.parse.quote_plus(base64.b64encode(digest))


def send_dingtalk(webhook_url: str, secret: str, title: str, text: str) -> bool:
    """发送 Markdown 消息到钉钉自定义机器人。

    - secret 非空时启用加签（追加 ``timestamp`` + ``sign`` 到 URL）。
    - 5 秒超时；任何异常或 ``errcode != 0`` 返回 False，不抛异常。
    """
    url = webhook_url
    if secret:
        ts = round(time.time() * 1000)
        url = f"{webhook_url}&timestamp={ts}&sign={_sign(secret, ts)}"
    payload: dict[str, Any] = {
        "msgtype": "markdown",
        "markdown": {"title": title, "text": text},
    }
    # 传输层异常重试 1 次（间隔 2s）：实测到 oapi.dingtalk.com 的 TLS 握手偶发
    # SSLEOFError（约 1/3 概率），新连接重试即可恢复；errcode != 0 属服务端
    # 确定性拒绝，不重试。成功路径仍只发一次（test_send_dingtalk_plain_text
    # 断言单次调用）。
    last_err: Exception | None = None
    for attempt in (1, 2):
        try:
            resp = requests.post(url, json=payload, timeout=_TIMEOUT)
            data = resp.json()
            if isinstance(data, dict) and data.get("errcode") == 0:
                return True
            log.warning("钉钉推送失败: %s", data)
            return False
        except Exception as e:
            last_err = e
            if attempt == 1:
                time.sleep(2)
    log.warning("钉钉推送异常: %s", last_err)
    return False
