#!/usr/bin/env python3
"""每日 09:25:30 集合竞价快照采集。

在竞价撮合完成后（09:25:00）、连续竞价开始前（09:30:00）调用，
腾讯行情返回的 price/volume 即竞价撮合价和竞价量。

用法（crontab 或 systemd timer 或 stockdata scheduler）：
    .venv/bin/python3 scripts/fetch_auction.py                # 全市场
    .venv/bin/python3 scripts/fetch_auction.py --codes 000989.XSHE,600519.XSHG
"""
from __future__ import annotations

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from app.quant.auction_cache import fetch_and_save


def main():
    import argparse
    ap = argparse.ArgumentParser(description="集合竞价快照采集")
    ap.add_argument("--codes", default=None, help="逗号分隔的标的列表（缺省=全市场）")
    ap.add_argument("--day", default=None, help="日期 YYYY-MM-DD（缺省=今天）")
    a = ap.parse_args()
    import datetime as _dt
    day = _dt.date.fromisoformat(a.day) if a.day else _dt.date.today()
    codes = a.codes.split(",") if a.codes else None
    df = fetch_and_save(day=day, codes=codes)
    print(f"auction: {len(df)} 只已保存 ({day})")


if __name__ == "__main__":
    main()
