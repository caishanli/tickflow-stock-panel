"""离线回放：复用 rqalpha_bridge 跑历史（与回测同一条路径）。"""
from __future__ import annotations

from ..rqalpha_bridge import run_backtest
from ..datasource.manager import QuantDataProvider


def run_replay(strategy_code: str, params: dict) -> dict:
    provider = QuantDataProvider()
    return run_backtest(strategy_code, params, provider=provider)
