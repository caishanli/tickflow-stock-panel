"""repair_etf_daily.py — 修复服务器 kline_etf_daily 残缺分区并校验。

背景：服务器 2026-06-29/30、07-01、07-06 的 ETF 日线分区只落到 1 只
（全市场 1600+ 只缺失），导致模拟盘流动性阈值/池过滤偏移 → 收益分叉。
这些分区"目录存在但内容残缺"，增量回源判定不缺、不会重写，须强制补。

用法（在服务器 backend/ 下，先 git pull 同步本脚本）：
  uv run python scripts/repair_etf_daily.py <check|repair|verify>

  check  只检查并打印各目标日期分区行数（不写数据）
  repair 对 4 天强制 sync_daily 回源（读旧→concat→unique 覆盖，幂等无副作用）
  verify 修完后复查行数（应 ≈1600+）

补完后再手动 reset 模拟盘（会从 07-10 自动重跑）：
  curl -b cookies POST /api/quant/sim/accounts/ed6ccd5c/reset
  然后 POST /api/quant/sim/accounts/ed6ccd5c/start
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

BACKEND = os.path.dirname(os.path.abspath(__file__))
if os.path.isdir(os.path.join(BACKEND, "app")):
    sys.path.insert(0, BACKEND)

TARGETS = [date(2026, 6, 29), date(2026, 6, 30), date(2026, 7, 1), date(2026, 7, 6)]


def _row_count(day: date) -> int:
    import polars as pl
    from app.config import settings
    root = settings.data_dir / "kline_etf_daily" / f"date={day.isoformat()}"
    if not root.is_dir():
        return -1  # 分区不存在
    df = pl.read_parquet(root / "*.parquet")
    return df.height


def check() -> None:
    from app.services import mootdx_service
    print(f"ETF 宇宙大小: {len(mootdx_service._etf_universe())}")
    for d in TARGETS:
        n = _row_count(d)
        print(f"  {d} 分区行数 {n}  {'OK' if n >= 1500 else '残缺/缺失!'}")


def repair() -> None:
    from app.services import mootdx_service
    for d in TARGETS:
        print(f"[repair] {d} sync_daily ...", flush=True)
        try:
            res = mootdx_service.sync_daily(d)
            n = _row_count(d)
            print(f"[repair] {d} → 分区行数={n}, written={res}")
        except Exception as e:  # noqa: BLE001
            print(f"[repair] {d} 失败: {e}", flush=True)


def verify() -> None:
    print("=== verify ===")
    ok = True
    for d in TARGETS:
        n = _row_count(d)
        print(f"  {d} 分区行数 {n}")
        if n < 1500:
            ok = False
    print("结论:", "全部补齐" if ok else "仍有缺失，请检查网络/日志")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["check", "repair", "verify"])
    args = ap.parse_args()
    {"check": check, "repair": repair, "verify": verify}[args.mode]()


if __name__ == "__main__":
    main()