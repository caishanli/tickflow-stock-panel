"""从 baostock 拉全市场股票复权因子事件 → 本机逐日 ex_factor 因子表。

## 为什么用 baostock 而不是归档的 adj_factor 列

归档 `4.21更新` 日线自带 `adj_factor` 列（baostock 后复权累计因子），但它是
**2026-04-18 的快照**，只到 2026-04-17。baostock 是归档的**同源上游**（实测
000001.SZ 事件日一一对应：2016-06-16 / 2017-07-21 / ... / 2025-06-12 / 2025-10-15），
且**覆盖到 2026-09-24**，多出约 5 个月。

## 口径

baostock `query_adjust_factor` 返回事件行：
  [code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor]

- `foreAdjustFactor` **已经是「最新锚定的前复权因子」**（最后事件日 = 1.000000），
  与本机契约（`adj_factor_etf` / `apply_qfq_daily`：最新=1.0、越往前越小）**同构**，
  不需要再做归一化换算。
- 事件日的因子是「**除权后**」的值（step function 在事件日当天跳变）——
  与归档因子表实测一致。

## 展开成逐日表

因子在两个事件日之间恒定 → 对每只标的，事件行 fore 值向前填充到上一个事件日，
最后补到「最后事件日之后的所有交易日」（值 = 1.0，因为最后事件日 fore=1.0）。

用法:
    uv run python -m app.quant.baostock_factor_sync --out ../data/adj_factor/all.parquet
    uv run python -m app.quant.baostock_factor_sync --limit 50 --dry-run
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date as _date
from pathlib import Path

import polars as pl

from app.services.baostock_backfill import (
    _bs,
    _guarded,
    query_adjust_factor_rows,
    stock_universe,
)

BS_PREFIX = {"SZ": "sz.", "SH": "sh.", "BJ": "bj."}


def to_bs_code(symbol: str) -> str:
    pure, suffix = symbol.split(".", 1)
    return BS_PREFIX.get(suffix.upper(), "sh.") + pure


def fetch_events(symbol: str, start: str, end: str) -> list[tuple[_date, float]]:
    """单只标的的 (除权日, foreAdjustFactor) 事件列表。"""
    code = to_bs_code(symbol)
    rows = _guarded(
        lambda: query_adjust_factor_rows(code, start, end, timeout=60, retries=1),
        timeout=90,
    )
    out: list[tuple[_date, float]] = []
    for r in rows:
        if len(r) < 3 or not r[1]:
            continue
        try:
            f = float(r[2])
        except (TypeError, ValueError):
            continue
        if f <= 0:
            continue
        try:
            out.append((_date.fromisoformat(r[1]), f))
        except ValueError:
            continue
    return sorted(set(out))


def expand_to_daily(symbol: str, events: list[tuple[_date, float]],
                    start: _date, end: _date) -> list[tuple[_date, float]]:
    """事件 → 逐日 ex_factor（step function：事件日当天起生效，向后保持到下一事件）。

    事件日 d 的 fore 值适用于 [d, 下一个事件日) 区间；最后一个事件适用到 end（含）。
    首个事件**之前**的日期沿用首个事件的值（该值已含全部历史事件的累计，
    因为 fore 是「锚定最新」的累计因子，事件日当天起即为该值）。

    实测（2026-10-21，000001.SZ）：与归档因子表的事件乘数逐个一致（0 不一致），
    baostock 额外覆盖 2016 年前的历史事件与 2026-04 之后的新分红。
    """
    if not events:
        return []
    out: list[tuple[_date, float]] = []
    evs = sorted(events)
    for i, (d, f) in enumerate(evs):
        nxt = evs[i + 1][0] if i + 1 < len(evs) else None
        stop = nxt if nxt is not None else end
        if stop < start:
            continue
        cur = d if d >= start else start
        while cur < stop or (nxt is None and cur == stop):
            out.append((cur, f))
            if cur == end:
                break
            cur = cur.fromordinal(cur.toordinal() + 1)
    # 首个事件之前的日期：沿用首个事件的值。
    # fore 是「锚定最新」的累计因子——事件日当天起即为该值，之前的历史事件已含在内；
    # 真实回源时 baostock 能取到 1991 年起的完整事件史，首事件远早于表起点，
    # 故此分支只在人工构造数据时出现。
    first_d, first_f = evs[0]
    if first_d > start:
        cur = start
        while cur < first_d:
            out.append((cur, first_f))
            cur = cur.fromordinal(cur.toordinal() + 1)
    out.sort()
    return out


def build_table(symbols: list[str], query_start: _date, end: _date,
                table_start: _date | None = None,
                progress_every: int = 200) -> tuple[pl.DataFrame, dict]:
    """全市场回源 → 逐日因子表。返回 (DataFrame, stats)。

    query_start = 回源起点（取完整事件史）；table_start = 因子表实际起始日
    （= query_start 时则整段输出）。
    """
    bs = _bs()
    _guarded(bs.login, timeout=30)
    stats: dict = {"symbols": len(symbols), "ok": 0, "no_events": 0, "failed": 0,
             "failed_samples": [], "bj_skipped": 0}
    frames: list[pl.DataFrame] = []
    t0 = time.time()
    try:
        for i, sym in enumerate(symbols, 1):
            if sym.endswith(".BJ"):
                stats["bj_skipped"] += 1     # baostock 不支持北交所
                continue
            try:
                evs = fetch_events(sym, query_start.isoformat(), end.isoformat())
            except Exception as exc:
                stats["failed"] += 1
                if len(stats["failed_samples"]) < 10:
                    stats["failed_samples"].append(f"{sym}: {str(exc)[:80]}")
                continue
            if not evs:
                stats["no_events"] += 1
                continue
            daily = expand_to_daily(sym, evs, table_start or query_start, end)
            if not daily:
                stats["no_events"] += 1
                continue
            frames.append(pl.DataFrame({
                "symbol": [sym] * len(daily),
                "trade_date": [d for d, _ in daily],
                "ex_factor": [f for _, f in daily],
            }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64}))
            stats["ok"] += 1
            if i % progress_every == 0:
                print(f"  [{i}/{len(symbols)}] ok={stats['ok']} 无事件={stats['no_events']} "
                      f"失败={stats['failed']} 用时={time.time()-t0:.0f}s", flush=True)
    finally:
        _guarded(bs.logout, timeout=30)
    if not frames:
        return pl.DataFrame(schema={"symbol": pl.String, "trade_date": pl.Date,
                                    "ex_factor": pl.Float64}), stats
    table = pl.concat(frames).sort(["symbol", "trade_date"])
    stats["rows"] = table.height
    stats["elapsed_s"] = round(time.time() - t0, 1)
    return table, stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/home/caisl/tickflow-stock-panel/data/adj_factor/all.parquet")
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--query-start", default="1990-01-01",
                    help="回源起点：必须早于表起点，才能拿到完整事件史"
                         "（fore 是累计因子，事件日当天起即为该值；缺早期事件会算错起始段）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 只（0=全市场）")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    start = _date.fromisoformat(args.start)
    end = _date.fromisoformat(args.end)
    query_start = _date.fromisoformat(args.query_start)
    symbols = stock_universe()
    if args.limit:
        symbols = symbols[: args.limit]

    table, stats = build_table(symbols, query_start, end, table_start=start)
    stats["out"] = args.out
    stats["table_start"] = start.isoformat()
    stats["query_start"] = query_start.isoformat()
    if not args.dry_run and not table.is_empty():
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        table.write_parquet(out, compression="zstd")
        stats["bytes"] = out.stat().st_size
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
