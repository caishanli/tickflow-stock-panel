"""拉取指数成分股的 mootdx xdxr 事件（含股本变化），落盘供回测时点股本使用。

输出: data/pools/stock_xdxr_events.parquet
  columns: symbol(6位), exdate(YYYYMMDD int), category, houzongguben(万股),
           panhouliutong(万股), songzhuangu(10送X)
幂等：重复运行覆盖写。
"""
import concurrent.futures as _cf
import datetime as _dt
import threading

import pandas as pd
import polars as pl

from app.quant.datasource.network_client import StockDataClient

OUT = "/home/caisl/tickflow-stock-panel/data/pools/stock_xdxr_events.parquet"
_print_lock = threading.Lock()


def fetch_one(sym6):
    """返回 [(sym6, yyyymmdd_int, category, hou_zgb_wan, pan_hou_lt_wan, songgu)]"""
    market = 1 if sym6.startswith(("6", "5", "9")) else 0
    try:
        from mootdx.quotes import Quotes
        client = Quotes.factory(market=market)
        df = client.xdxr(symbol=sym6)
        if df is None or len(df) == 0:
            return []
        out = []
        for _, r in df.iterrows():
            try:
                d = _dt.date(int(r["year"]), int(r["month"]), int(r["day"]))
            except Exception:
                continue
            def _f(v):
                try:
                    v = float(v)
                    return v if v == v else None
                except Exception:
                    return None
            out.append((sym6, int(d.strftime("%Y%m%d")), int(r["category"]),
                        _f(r.get("houzongguben")), _f(r.get("panhouliutong")),
                        _f(r.get("songzhuangu"))))
        return out
    except Exception:
        return []


def main():
    client = StockDataClient()
    universe = set()
    for idx in ("000300.XSHG", "399101.XSHE"):
        for c in client.get_index_stocks(idx):
            universe.add(str(c).split(".")[0])
    syms = sorted(universe)
    print(f"宇宙 {len(syms)} 只，开始拉 xdxr…")
    rows = []
    done = 0
    with _cf.ThreadPoolExecutor(max_workers=8) as ex:
        for res in ex.map(fetch_one, syms):
            rows.extend(res)
            done += 1
            if done % 200 == 0:
                with _print_lock:
                    print(f"  {done}/{len(syms)}")
    df = pd.DataFrame(rows, columns=["symbol", "exdate", "category",
                                     "houzongguben", "panhouliutong",
                                     "songzhuangu"])
    pl_df = pl.from_pandas(df)
    pl_df.write_parquet(OUT)
    n_sym = pl_df["symbol"].n_unique()
    cat5 = pl_df.filter(pl.col("category") == 5).height
    print(f"落盘 {pl_df.height} 行 / {n_sym} 只（cat5 股本变化 {cat5} 条）→ {OUT}")


if __name__ == "__main__":
    main()
