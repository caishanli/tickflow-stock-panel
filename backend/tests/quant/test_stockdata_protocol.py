import socket

from app.services.stockdata import protocol


def test_request_roundtrip():
    raw = protocol.encode_request(7, "get_price", {"security": "512670.XSHG", "frequency": "daily"})
    c, s = socket.socketpair()
    s.sendall(raw)
    got = protocol.decode_frame(c)
    c.close(); s.close()
    assert got["v"] == 1 and got["id"] == 7
    assert got["m"] == "get_price"
    assert got["p"]["security"] == "512670.XSHG"


def test_response_roundtrip_json():
    raw = protocol.encode_response(3, True, "json", {"pong": True})
    assert protocol.decode_response(raw)["ok"] is True
    assert protocol.decode_response(raw)["d"] == {"pong": True}


def test_response_parquet():
    import io
    import polars as pl
    df = pl.DataFrame({"symbol": ["a"], "close": [1.0]})
    raw = protocol.encode_response(1, True, "parquet", df)
    msg = protocol.decode_response(raw)
    assert msg["t"] == "parquet"
    back = pl.read_parquet(io.BytesIO(msg["d"]))
    assert back["close"].to_list() == [1.0]
