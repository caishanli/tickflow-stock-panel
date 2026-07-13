from app.quant.datasource.base import DataSourceError
from app.quant.datasource.manager import QuantDataProvider


class _FailSource:
    name = "fail"

    def get_daily(self, *a, **k):
        raise DataSourceError("boom")

    def get_minute(self, *a, **k):
        raise DataSourceError("boom")


class _OkSource:
    name = "ok"

    def __init__(self, df):
        self._df = df

    def get_daily(self, *a, **k):
        return self._df

    def get_minute(self, *a, **k):
        return self._df


def test_fallback_to_second_source():
    import pandas as pd
    ok = _OkSource(pd.DataFrame({"close": [1.0]}))
    prov = QuantDataProvider.__new__(QuantDataProvider)
    prov.sources = {"fail": _FailSource(), "ok": ok}
    prov.priority = ["fail", "ok"]
    prov.cache = type("C", (), {"get": lambda *a, **k: None, "put": lambda *a, **k: None})()
    df = prov.fetch("get_daily", "X")
    assert list(df["close"]) == [1.0]


def test_all_fail_raises():
    prov = QuantDataProvider.__new__(QuantDataProvider)
    prov.sources = {"fail": _FailSource()}
    prov.priority = ["fail"]
    prov.cache = type("C", (), {"get": lambda *a, **k: None, "put": lambda *a, **k: None})()
    try:
        prov.fetch("get_daily", "X")
        assert False, "should raise"
    except DataSourceError:
        pass
