"""网络数据源：把 StockDataClient 适配成 DataSource 接口（喂给 DataManager.fetch）。"""
from __future__ import annotations

import logging

import pandas as pd

from ...datasource.base import DataSource, DataSourceError

log = logging.getLogger("app.quant.jqengine.datasource.network_source")


class NetworkSource(DataSource):
    name = "network"

    def __init__(self, client=None):
        from app.quant.datasource.network_client import StockDataClient
        self.client = client or StockDataClient()

    def _fetch_daily(self, code, start, end) -> pd.DataFrame:
        out = self.client.get_price(code, start_date=start, end_date=end,
                                    frequency="daily")
        df = out.get(code)
        if df is None or df.empty:
            raise DataSourceError(f"网络无日线数据: {code}")
        return df

    def get_daily(self, code, start, end):
        return self._fetch_daily(code, start, end)

    def get_minute(self, code, date=""):
        end = date or pd.Timestamp.now().normalize().date()
        start = (pd.Timestamp(end) - pd.Timedelta(days=15)).date()
        out = self.client.get_price(code, start_date=start, end_date=end,
                                    frequency="1m")
        df = out.get(code)
        if df is None or df.empty:
            raise DataSourceError(f"网络无分钟数据: {code}")
        return df

    def get_stock_list(self):
        df = self.client.get_all_securities(types=["stock"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]

    def get_etf_list(self):
        df = self.client.get_all_securities(types=["etf"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]

    def get_stock_names(self):
        return self.client.get_stock_names() or {}

    def test_connection(self):
        try:
            self.client.ping()
            return True, "stockdata 服务连接正常"
        except Exception as e:  # noqa: BLE001
            return False, str(e)
