"""网络数据源看护模式适配（改编自 quant-daydayup datasource/manager.py）。"""
from __future__ import annotations

import pandas as pd

from .base import DataSourceError


class QuantDataProvider:
    """网络数据源看护模式适配：一切数据走 StockDataClient（零本地文件/零直连）。"""

    def __init__(self, client=None):
        from app.quant.datasource.network_client import StockDataClient
        self.client = client or StockDataClient()

    def get_daily(self, code, start, end):
        out = self.client.get_price(code, start_date=start, end_date=end, frequency="daily")
        df = out.get(code)
        if df is None or df.empty:
            raise DataSourceError(f"网络无日线数据: {code}")
        return df

    def get_minute(self, code, date):
        out = self.client.current_snapshot([code], as_of=f"{date} 15:00:00")
        df = out.get(code)
        if df is None or df.empty:
            return pd.DataFrame()
        return df

    def get_stock_list(self):
        df = self.client.get_all_securities(types=["stock"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]

    def get_etf_list(self):
        df = self.client.get_all_securities(types=["etf"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]
