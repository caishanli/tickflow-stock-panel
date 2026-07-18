"""验证本地分钟缓存的 real_<code> 在断裂日是否被聚宽数据填充。"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.quant.jqengine.datasource.cache import DataCache

cache = DataCache()
codes = ["159929.XSHE", "159934.XSHE", "510720.XSHG", "513100.XSHG"]
for code in codes:
    df = cache.peek("minute", f"real_{code}")
    if df is None:
        print(code, "NO DATA")
        continue
    df = df.sort_index()
    # 找 2026-02-24 和 2026-05-06 附近
    for d in ["2026-02-24", "2026-05-06"]:
        day = pd.Timestamp(d)
        sub = df[(df.index >= day) & (df.index < day + pd.Timedelta(days=1))]
        if sub.empty:
            print(f"{code} {d}: 仍 EMPTY")
        else:
            first, last = sub.index.min(), sub.index.max()
            n = len(sub)
            print(f"{code} {d}: {n} bars, {first} ~ {last}, "
                  f"close[0]={sub['close'].iloc[0]:.4f}")
    # 检查间隔：只看跨自然日 > 1 天的异常间隔(排除周末/午休/正常隔夜)
    if isinstance(df.index, pd.DatetimeIndex):
        s = df.index.to_series().sort_values()
        gaps = s.diff().dropna()
        # 隔夜正常 ~18h; 周末 ~3天; 异常长间隔 = >4 天(即之前 genuine gap)
        anomaly = gaps[gaps > pd.Timedelta(days=4)]
        print(f"  {code}: 总 bar={len(df)}, 异常长间隔(>4天)次数={len(anomaly)}")
        for t, g in anomaly.items():
            print(f"    {t} 间隔 {g}")
    print()
