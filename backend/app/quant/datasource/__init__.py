"""量化多源数据层（改编自 quant-daydayup datasource）。

暴露统一接口与优先级降级调度器，供回测 / 模拟盘子系统使用。
"""
from .base import DataSource, DataSourceError
from .cache import DataCache
from .manager import QuantDataProvider
from .tickflow_src import TickflowSource

__all__ = [
    "DataSource",
    "DataSourceError",
    "DataCache",
    "QuantDataProvider",
    "TickflowSource",
]
