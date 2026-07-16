"""恢复被测试污染的两个 real_ 键。

1. 删除 real_159929.XSHE / real_180601.XSHE（测试假数据）；
2. 从 mootdx 重新拉真实分钟，写回（仅当 mootdx 能取到）。
"""
import pandas as pd

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.mootdx_src import MootdxSource

c = DataCache()
moo = MootdxSource()

for code in ["159929.XSHE", "180601.XSHE"]:
    key = f"real_{code}"
    # 删除旧键（无论真假）—— cache 无 delete，直接走 sqlite
    try:
        conn = c._conn("minute")
        conn.execute("DELETE FROM cache WHERE key=?", (key,))
        conn.commit()
        print(f"已删除 {key}")
    except Exception as e:
        print(f"删除 {key} 失败: {e}")
    # 从 mootdx 重新拉真实数据
    try:
        df = moo.get_minute(code)
    except Exception as e:
        print(f"mootdx 拉 {code} 失败: {e}")
        continue
    if df is None or df.empty:
        print(f"mootdx 无 {code} 数据，跳过（键保持删除态）")
        continue
    c.put("minute", key, df)
    print(f"已用 mootdx 真实数据重写 {key}: {len(df)} 行, "
          f"{df.index.min()} ~ {df.index.max()}")
