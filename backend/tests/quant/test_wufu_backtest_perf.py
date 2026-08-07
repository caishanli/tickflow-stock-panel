"""集成：wufu-v5.2 回测 260401-260716 全程 ≤120s（需真实 data/，非默认运行）。

标记 @pytest.mark.integration，默认跳过；CI/验收时显式跑：
  uv run --extra dev pytest -m integration tests/quant/test_wufu_backtest_perf.py -q
"""
import os
import time

import pytest

from app.quant.jqengine.datasource.manager import DataManager

pytestmark = pytest.mark.integration

STRATEGY = "tests/fixtures/wufu_v52/wufu-v5.2.py"
START, END = "2026-04-01", "2026-07-16"
PERF_BUDGET_S = 120


@pytest.mark.skipif(
    not os.path.isdir(os.path.join(DataManager._partition_root(), "kline_daily")),
    reason="需要真实 data/ 分区",
)
def test_wufu_backtest_within_120s():
    from app.quant.rqalpha_bridge import run_jq_backtest
    t0 = time.monotonic()
    run_jq_backtest(STRATEGY, {"start": START, "end": END,
                               "benchmark": "510300.XSHG", "minute_cache_cap": 800},
                    db_path="data/quant.db")
    elapsed = time.monotonic() - t0
    assert elapsed <= PERF_BUDGET_S, f"回测耗时 {elapsed:.1f}s 超过 {PERF_BUDGET_S}s 预算"
