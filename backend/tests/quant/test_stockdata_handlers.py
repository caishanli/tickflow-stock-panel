import datetime as _dt
import os

import pytest

from app.services.stockdata.handlers import HANDLERS, handle
from app.services.stockdata.sources import DataSources


@pytest.fixture
def src(tmp_path):
    import polars as pl
    day = _dt.date.today().isoformat()
    d = os.path.join(str(tmp_path), "kline_etf_minute", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame({
        "symbol": ["512670.SH"], "datetime": [f"{day} 10:00:00"],
        "open": [1.0], "high": [1.1], "low": [0.9], "close": [1.05],
        "volume": [1000], "amount": [1050.0],
    }).write_parquet(os.path.join(d, "part.parquet"))
    return DataSources(data_root=str(tmp_path), mootdx_factory=None)


def test_handlers_registered():
    for m in ("ping", "status", "get_price", "current_snapshot", "preload_daily",
              "get_minute", "get_trade_days", "get_all_securities",
              "get_security_info", "get_index_stocks", "get_stock_names",
              "get_adj_factors", "trigger_sync"):
        assert m in HANDLERS, m


def test_get_price_minute(src):
    t, data = handle("get_price", {"security": "512670.XSHG",
                                   "frequency": "1m"}, src)
    assert t == "parquet"
    assert data["close"].to_list() == [1.05]


def test_ping(src):
    t, data = handle("ping", {}, src)
    assert t == "json" and data["pong"] is True
