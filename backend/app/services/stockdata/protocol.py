"""TCP 帧编解码：4 字节大端长度前缀 + msgpack 负载。

请求：  {"v":1, "id":<int>, "m":<method>, "p":{params}}
响应：  {"v":1, "id":<int>, "ok":<bool>, "t":"parquet"|"json", "d":<bytes|dict>}
parquet 响应的 ``d`` 为原始 parquet 字节（嵌入 msgpack 二进制），
json 响应的 ``d`` 为 dict/list。
"""
from __future__ import annotations

import io

import msgpack
import polars as pl

VERSION = 1
_HEADER = 4


def encode_request(req_id: int, method: str, params: dict) -> bytes:
    payload = msgpack.packb(
        {"v": VERSION, "id": req_id, "m": method, "p": params},
        use_bin_type=True,
    )
    return len(payload).to_bytes(_HEADER, "big") + payload


def _recv_exact(conn, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("连接已关闭")
        buf.extend(chunk)
    return bytes(buf)


def decode_frame(conn) -> dict:
    """从 socket 阻塞读一帧并解码（线程内调用）。"""
    header = _recv_exact(conn, _HEADER)
    n = int.from_bytes(header, "big")
    payload = _recv_exact(conn, n)
    msg = msgpack.unpackb(payload, raw=False)
    if msg.get("v") != VERSION:
        raise ValueError(f"协议版本不匹配: {msg.get('v')}")
    return msg


def encode_response(req_id: int, ok: bool, t: str, data) -> bytes:
    if t == "parquet":
        buf = io.BytesIO()
        data.write_parquet(buf)
        body = buf.getvalue()
        payload = msgpack.packb(
            {"v": VERSION, "id": req_id, "ok": ok, "t": "parquet", "d": body},
            use_bin_type=True,
        )
    else:
        payload = msgpack.packb(
            {"v": VERSION, "id": req_id, "ok": ok, "t": "json", "d": data},
            use_bin_type=True,
        )
    return len(payload).to_bytes(_HEADER, "big") + payload


def decode_response(data: bytes) -> dict:
    payload = data[_HEADER:]
    return msgpack.unpackb(payload, raw=False)
