"""钉钉自定义机器人消息发送（Markdown + 加签，fire-and-forget）。

发送失败永不抛异常：调用方只需关心 True/False，适合模拟盘定时推送场景。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
import urllib.parse
from typing import Any

import requests

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
    try:
        resp = requests.post(url, json=payload, timeout=_TIMEOUT)
        data = resp.json()
    except Exception:
        return False
    return data.get("errcode") == 0
