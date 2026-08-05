import datetime as _dt
import os
import threading

import pandas as pd
import polars as pl
import pytest

from app.quant.datasource.network_client import StockDataClient
from app.services.stockdata.server import StockDataServer
from app.services.stockdata.sources import DataSources


@pytest.fixture
def server_and_client(tmp_path, monkeypatch):
    root = tmp_path / "sd"
    day = _dt.date.today().isoformat()
    for i, (sub, sym, ts, close) in enumerate((
        ("kline_etf_minute", "512670.SH", f"{day} 10:00:00", 1.05),
        ("kline_etf_minute", "159919.SZ", f"{day} 10:00:00", 3.10),
    )):
        d = os.path.join(str(root), sub, f"date={day}")
        os.makedirs(d, exist_ok=True)
        pl.DataFrame({
            "symbol": [sym], "datetime": [ts], "open": [close], "high": [close],
            "low": [close], "close": [close], "volume": [1000], "amount": [close * 1000.0],
        }).write_parquet(os.path.join(d, f"part-{i}.parquet"))
    # 非交易时段门控：current_snapshot 不触网（fixture 无 mootdx，测试只验读路径）
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    srv = StockDataServer(("127.0.0.1", 0), DataSources(data_root=str(root), mootdx_factory=None))
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    cli = StockDataClient(port=port)
    yield cli, port
    cli.close()
    srv.shutdown()
    srv.server_close()


def test_ping(server_and_client):
    cli, _ = server_and_client
    assert cli.ping()["pong"] is True


def test_get_price_minute_multi(server_and_client):
    cli, _ = server_and_client
    out = cli.get_price(["512670.XSHG", "159919.XSHE"], frequency="1m",
                        start_date=_dt.date.today().isoformat(),
                        end_date=_dt.date.today().isoformat())
    assert set(out) == {"512670.XSHG", "159919.XSHE"}
    assert out["512670.XSHG"]["close"].iloc[0] == 1.05
    assert isinstance(out["512670.XSHG"].index, pd.DatetimeIndex)


def test_current_snapshot(server_and_client):
    cli, _ = server_and_client
    snap = cli.current_snapshot(["512670.XSHG", "159919.XSHE"])
    assert "512670.XSHG" in snap
    assert snap["512670.XSHG"]["close"].iloc[-1] == 1.05


def test_business_error_raises_runtime_error_and_keeps_connection(server_and_client):
    cli, _ = server_and_client
    connects = []
    orig_connect = StockDataClient._connect

    def counting_connect(self):
        connects.append(1)
        return orig_connect(self)

    StockDataClient._connect = counting_connect
    try:
        with pytest.raises(RuntimeError) as ei:
            cli._request("no_such_method", {})
        assert "未知 method" in str(ei.value)
        assert cli._sock is not None
        assert cli.ping()["pong"] is True
        assert len(connects) == 1
    finally:
        StockDataClient._connect = orig_connect
