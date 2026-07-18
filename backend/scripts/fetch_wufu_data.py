#!/usr/bin/env python3
"""预下载 五福闹新春-v5.2 回测所需行情到本地缓存（daily / 5min parquet）。

从 wufu-v5.2.py 抽取固定 ETF 池（全球+中国），并补齐基准/指数/防御 ETF，
用 DataManager（tushare 日线 + baostock 5分钟插值）拉取并落盘，使回测离线可复现。

用法:
  uv run python scripts/fetch_wufu_data.py [--start 2026-01-01] [--end 2026-07-08] [--minute]
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from app.quant.jqengine.datasource.manager import DataManager  # noqa: E402
from app.quant.jqengine.datasource.base import DataSourceError  # noqa: E402

STRATEGY = "/home/caisl/五福闹新春-v5.2/wufu-v5.2.py"
AUX = [
    "510300.XSHG",  # 基准
    "000300.XSHG", "399101.XSHE", "399006.XSHE", "000510.XSHG",  # 走弱期判定指数
    "511880.XSHG",  # 防御 ETF
]


def extract_pool(strategy_path):
    with open(strategy_path, encoding="utf-8") as f:
        txt = f.read()
    codes = re.findall(r"\b(\d{6}\.(?:XSHG|XSHE))\b", txt)
    seen = []
    for c in codes:
        if c not in seen:
            seen.append(c)
    return seen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-07-08")
    ap.add_argument("--daily-start", default="2025-01-01",
                    help="日线回看起点（覆盖策略 ~250 日动量回看）")
    ap.add_argument("--minute", action="store_true", help="同时下载 5 分钟线（baostock）")
    args = ap.parse_args()

    dm = DataManager()
    # 沙箱屏蔽了 mootdx 真实 1 分钟服务器：禁用真实分钟回源，直接用 baostock 5 分钟插值
    dm._use_real_minute = False

    codes = extract_pool(STRATEGY) + AUX
    # 去重保序
    uniq = list(dict.fromkeys(codes))
    print(f"[fetch] 标的池大小: {len(uniq)}")

    s = args.daily_start.replace("-", "")
    e = args.end.replace("-", "")
    ok = 0
    fail = 0
    for i, c in enumerate(uniq):
        try:
            df = dm.fetch("get_daily", c, s, e)
            if df is not None and len(df):
                ok += 1
            else:
                fail += 1
                print(f"[daily] 空: {c}")
        except Exception as ex:
            fail += 1
            print(f"[daily] 失败 {c}: {ex}")
        if (i + 1) % 40 == 0:
            print(f"[daily] 进度 {i + 1}/{len(uniq)} (ok={ok})")
    print(f"[daily] 完成 ok={ok} fail={fail}")

    if args.minute:
        dm.set_minute_window(args.start, args.end)
        ok = 0
        fail = 0
        for i, c in enumerate(uniq):
            try:
                df = dm.get_minute(c, args.end, args.start)
                if df is not None and len(df):
                    ok += 1
                else:
                    fail += 1
            except Exception as ex:
                fail += 1
                print(f"[minute] 失败 {c}: {ex}")
            if (i + 1) % 40 == 0:
                print(f"[minute] 进度 {i + 1}/{len(uniq)} (ok={ok})")
        print(f"[minute] 完成 ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
