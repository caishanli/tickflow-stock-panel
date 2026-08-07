import pandas as pd
import pytest

from app.quant.datasource.base import DataSourceError
from app.quant.datasource.manager import QuantDataProvider


class _EmptyClient:
    def get_price(self, security, start_date=None, end_date=None, frequency="daily", fields=None):
        return {}

    def current_snapshot(self, codes, as_of=None):
        return {}


def test_get_daily_empty_raises():
    prov = QuantDataProvider(client=_EmptyClient())
    with pytest.raises(DataSourceError):
        prov.get_daily("600000.XSHG", "2026-01-01", "2026-02-01")


def test_get_minute_empty_returns_empty_df():
    prov = QuantDataProvider(client=_EmptyClient())
    df = prov.get_minute("600000.XSHG", "2026-01-01")
    assert isinstance(df, pd.DataFrame)
    assert df.empty
