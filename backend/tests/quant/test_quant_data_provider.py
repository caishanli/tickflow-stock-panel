import datetime as _dt

import pandas as pd

from app.quant.datasource.manager import QuantDataProvider


def _df(closes):
    idx = pd.date_range(_dt.date.today() - pd.Timedelta(days=len(closes) - 1),
                        periods=len(closes))
    return pd.DataFrame({"open": closes, "high": closes, "low": closes,
                         "close": closes, "volume": 1000.0, "amount": 1e5}, index=idx)


class FakeClient:
    def get_price(self, security, start_date=None, end_date=None, frequency="daily", fields=None):
        codes = security if isinstance(security, list) else [security]
        return {c: _df([1.0, 1.1]) for c in codes}

    def current_snapshot(self, codes, as_of=None):
        return {c: _df([1.05]) for c in codes}


def test_provider_get_daily():
    p = QuantDataProvider(client=FakeClient())
    df = p.get_daily("512670.XSHG", "2026-01-01", "2026-02-01")
    assert df["close"].iloc[-1] == 1.1


def test_provider_get_minute():
    p = QuantDataProvider(client=FakeClient())
    df = p.get_minute("512670.XSHG", str(_dt.date.today()))
    assert df["close"].iloc[-1] == 1.05
