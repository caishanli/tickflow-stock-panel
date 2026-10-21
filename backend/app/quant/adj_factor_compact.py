"""因子表压缩：逐日表 → 事件行表（step function 稀疏表示）。

因子是 step function（事件之间恒定）→ 逐日表里 99.8% 是冗余。
事件行表 = 每只标的的「首行（基准）+ 变点」，体积降 100~600x。

消费方两种表示都支持（服务端 apply_qfq_daily 用 as-of backward join，
对逐日表等价、对事件表正确；客户端 _adj_events 取相邻行比值 = 事件乘数）。

用法:
    uv run python -m app.quant.adj_factor_compact --in ../data/adj_factor_etf/all.parquet
    uv run python -m app.quant.adj_factor_compact --in X --out Y --dry-run
"""

from __future__ import annotations

import argparse
import json
from datetime import date as _date
from pathlib import Path

import polars as pl

from app.quant.baostock_factor_sync import daily_to_events


def compact(daily: pl.DataFrame, table_start: _date | None = None,
            min_change: float = 0.0) -> pl.DataFrame:
    """逐日因子表 → 事件行表。

    table_start 给定时额外补一行该日的基准（全局起始日）；为 None 时
    每只标的以其自身首行为基准（事件行表天然含 is_first 行）。
    min_change：噪声过滤阈值（相对变化），按来源分别取（见 daily_to_events）。
    """
    start = table_start or daily["trade_date"].min()
    if not isinstance(start, _date):
        start = _date.fromisoformat(str(start))
    out = daily_to_events(daily, start, min_change)
    return out.sort(["symbol", "trade_date"])

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", required=True, help="逐日因子表路径")
    ap.add_argument("--out", dest="dst", default="", help="输出路径（默认原地覆盖）")
    ap.add_argument("--table-start", default="", help="表起始日 YYYY-MM-DD（补全局基准行）")
    ap.add_argument("--min-change", type=float, default=0.0,
                    help="噪声过滤阈值（相对变化 |F(t)/F(t-1)-1|）："
                         "mootdx ETF 表用 1e-3（其噪声底），baostock 表用 1e-4")
    ap.add_argument("--backup", default="", help="覆盖前备份到该路径")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    src = Path(args.src)
    dst = Path(args.dst) if args.dst else src
    daily = pl.read_parquet(src)
    start = _date.fromisoformat(args.table_start) if args.table_start else None
    out = compact(daily, start, args.min_change)
    stats = {
        "in": str(src), "out": str(dst),
        "daily_rows": daily.height, "event_rows": out.height,
        "symbols": out["symbol"].n_unique(),
        "compression": round(daily.height / max(out.height, 1), 1),
    }
    if not args.dry_run:
        if args.backup and dst.exists():
            Path(args.backup).parent.mkdir(parents=True, exist_ok=True)
            # 跨设备不能 rename（EXDEV），用 copy2
            import shutil
            shutil.copy2(dst, args.backup)
        dst.parent.mkdir(parents=True, exist_ok=True)
        out.write_parquet(dst, compression="zstd")
        stats["bytes"] = dst.stat().st_size
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
