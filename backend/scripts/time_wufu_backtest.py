"""计时 wufu-v5.2 回测 260401-260716：总耗时 + 网络请求分段 + 引擎耗时。

用法（backend/ 下，需 stock data 服务 idle）：
  uv run --extra dev python scripts/time_wufu_backtest.py
输出各阶段耗时到 stdout，可对比热点。
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

STRATEGY = "tests/fixtures/wufu_v52/wufu-v5.2.py"
PARAMS = {"start": "2026-04-01", "end": "2026-07-16",
          "benchmark": "510300.XSHG", "minute_cache_cap": 800,
          "out_dir": "data/quant_sim/jqwufu_network"}

_t0 = time.monotonic()
from app.quant.rqalpha_bridge import run_jq_backtest
t_import = time.monotonic() - _t0
print(f"[timing] import rqalpha_bridge: {t_import:.1f}s")

from app.quant.jqengine.datasource.manager import DataManager
from app.quant.datasource.network_client import StockDataClient

_req_time = {}
_req_cnt = {}
_orig_request = StockDataClient._request
def _timed_request(self, method, params, retry=3):
    t0 = time.monotonic()
    r = _orig_request(self, method, params, retry=retry)
    dt = time.monotonic() - t0
    _req_time[method] = _req_time.get(method, 0.0) + dt
    _req_cnt[method] = _req_cnt.get(method, 0) + 1
    return r
StockDataClient._request = _timed_request

_freq_time = {}
_orig_gp = StockDataClient.get_price
_gp1m_codes = {}
_daily_reqs = []
def _timed_gp(self, security, start_date=None, end_date=None, frequency="daily",
              fields=None):
    t0 = time.monotonic()
    r = _orig_gp(self, security, start_date=start_date, end_date=end_date,
                 frequency=frequency, fields=fields)
    dt = time.monotonic() - t0
    codes = security if isinstance(security, (list, tuple)) else [security]
    _freq_time.setdefault(f"{frequency} x{len(codes)}", [0.0, 0])
    _freq_time[f"{frequency} x{len(codes)}"][0] += dt
    _freq_time[f"{frequency} x{len(codes)}"][1] += 1
    if frequency == "1m" and len(codes) == 1:
        for c in codes:
            _gp1m_codes[c] = _gp1m_codes.get(c, 0) + 1
    if frequency == "daily" and len(codes) == 1:
        _daily_reqs.append((codes[0], start_date, end_date, dt))
    return r
StockDataClient.get_price = _timed_gp

_orig_preload = DataManager.preload_daily
def _timed_preload(self, force=False):
    t0 = time.monotonic()
    r = _orig_preload(self, force=force)
    print(f"[timing] preload_daily (force={force}): {time.monotonic()-t0:.1f}s")
    return r
DataManager.preload_daily = _timed_preload

_orig_pool = DataManager.preload_minute_for_pool
def _timed_pool(self, codes, as_of=None):
    t0 = time.monotonic()
    r = _orig_pool(self, codes, as_of=as_of)
    print(f"[timing] preload_minute_for_pool n={len(codes)}: {time.monotonic()-t0:.1f}s")
    return r
DataManager.preload_minute_for_pool = _timed_pool

_phase = {}
def _wrap(cls, name, label=None):
    orig = getattr(cls, name)
    def wrapper(self, *a, **kw):
        t0 = time.monotonic()
        try:
            return orig(self, *a, **kw)
        finally:
            k = label or name
            _phase[k] = _phase.get(k, 0.0) + (time.monotonic() - t0)
    wrapper.__name__ = name
    setattr(cls, name, wrapper)

_wrap(DataManager, "get_minute_feed")
_wrap(DataManager, "_load_minute_from_partitions")
_wrap(DataManager, "_ensure_minute_windowed")
_wrap(DataManager, "get_minute")

_eng = {}
import rqalpha
_orig_run = rqalpha.run
def _timed_run(config, source_code=None, **kw):
    t0 = time.monotonic()
    try:
        return _orig_run(config, source_code=source_code, **kw)
    finally:
        _eng["rqalpha.run"] = _eng.get("rqalpha.run", 0.0) + (time.monotonic() - t0)
rqalpha.run = _timed_run

from app.quant.jqcompat import JqDataSource
_orig_jq_init = JqDataSource.__init__
def _timed_jq_init(self, *a, **kw):
    t0 = time.monotonic()
    try:
        return _orig_jq_init(self, *a, **kw)
    finally:
        _eng["JqDataSource.__init__"] = _eng.get("JqDataSource.__init__", 0.0) + (time.monotonic() - t0)
JqDataSource.__init__ = _timed_jq_init

t0 = time.monotonic()
result = run_jq_backtest(STRATEGY, PARAMS, db_path="data/quant.db")
elapsed = time.monotonic() - t0
print(f"\n===== 回测总耗时: {elapsed:.1f}s =====")
print("[timing] 网络请求明细（method: 次数 / 总耗时）")
for k, v in sorted(_req_time.items(), key=lambda kv: -kv[1]):
    print(f"  {k}: {_req_cnt[k]} 次 / {v:.1f}s")
print("[timing] get_price 频率细分（freq x批内标的数: 次数 / 总耗时）")
for k, (t, n) in sorted(_freq_time.items(), key=lambda kv: -kv[1][0]):
    print(f"  {k}: {n} 次 / {t:.1f}s")
print("[timing] DataManager 方法耗时")
for k, v in sorted(_phase.items(), key=lambda kv: -kv[1]):
    print(f"  {k}: {v:.1f}s")
print("[timing] 引擎耗时")
for k, v in _eng.items():
    print(f"  {k}: {v:.1f}s")
if _gp1m_codes:
    uniq = len(_gp1m_codes)
    repeat = sum(1 for v in _gp1m_codes.values() if v > 1)
    print(f"[timing] 1m x1 请求: 去重 {uniq} 只, 其中重复加载 {repeat} 只, 总 {sum(_gp1m_codes.values())} 次")
    for c, n in sorted(_gp1m_codes.items(), key=lambda kv: -kv[1])[:15]:
        print(f"    {c}: {n} 次")
from collections import Counter
_dc = Counter((c, w) for c, s, e, _ in _daily_reqs for w in [str(s)[:10]])
print(f"[timing] daily x1 请求: {len(_daily_reqs)} 次")
for (c, w), n in _dc.most_common(20):
    print(f"    {c} window={w}: {n} 次 ({sum(d for cc, ss, ee, d in _daily_reqs if cc==c and str(ss)[:10]==w):.1f}s)")
if isinstance(result, dict):
    for k, v in result.items():
        if k != "trades":
            print(f"  {k}: {v}")
