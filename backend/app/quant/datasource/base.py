"""数据源抽象基类与统一异常（改编自 quant-daydayup）。"""


class DataSourceError(Exception):
    """数据源不可用 / 无数据 / 超时 等统一错误。"""


class DataSource:
    name = "base"

    def get_daily(self, code, start, end):
        raise NotImplementedError

    def get_minute(self, code, date):
        raise NotImplementedError

    def get_index_realtime(self, codes):
        raise NotImplementedError

    def get_etf_list(self):
        raise NotImplementedError

    def get_stock_list(self):
        raise NotImplementedError

    def get_us_index(self):
        raise NotImplementedError

    def test_connection(self):
        raise NotImplementedError
