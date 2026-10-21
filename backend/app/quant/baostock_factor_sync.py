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

产物：**事件行表**（step function 稀疏表示：基准行 + 变点），落
``data/adj_factor_baostock/all.parquet`` —— 股票复权因子的权威源。
（不要写进 ``data/adj_factor/``：该目录会被服务端因子加载扫到，与权威源
混源会因归一化锚点不同而产生复权伪跳变，见 ``sources._load_adj_factors``。）

用法:
    uv run python -m app.quant.baostock_factor_sync            # 写事件行表到 adj_factor_baostock/
    uv run python -m app.quant.baostock_factor_sync --limit 50 --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date as _date
from pathlib import Path

import polars as pl

from app.services.baostock_backfill import (
    DATA_ROOT,
    _bs,
    _guarded,
    query_adjust_factor_rows,
    stock_universe,
)

BS_PREFIX = {"SZ": "sz.", "SH": "sh.", "BJ": "bj."}


def to_bs_code(symbol: str) -> str:
    """tushare 码 → baostock 码（sz./sh./bj.）。无后缀的按沪市处理。"""
    pure, _, suffix = str(symbol).rpartition(".")
    if not pure:
        return "sh." + str(symbol)
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
    首个事件**之前**的日期：取 ``fore(首事件) × m(首事件)``（m = 该事件的乘数
    = fore(首事件)/fore(次事件)）——累计因子在事件日**当天跳变**，事件之前的值
    必须更小；直接沿用 fore(首事件) 会让首个事件在 step function 上「不跳变」
    → ``daily_to_events`` 会把该事件整条丢掉（下游少一次除权折算）。

    例外：真实回源时 baostock 在查询起点会返回一条**基线行**（该日的因子），
    此时「首事件」就是基线、首事件之前没有区间，本分支不触发（实测全市场
    5531 只都有基线行）。只有一条事件时无法推 m，退回 fore(首事件)。

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
    # 首个事件之前的日期：值 = fore(首事件) × m(首事件)（累计因子在事件日跳变，
    # 事件之前更小）。只有一条事件时 m 不可推，退回 fore(首事件)。
    first_d, first_f = evs[0]
    if first_d > start:
        pre_f = first_f
        if len(evs) >= 2 and evs[1][1]:
            pre_f = first_f * (first_f / evs[1][1])
        cur = start
        while cur < first_d:
            out.append((cur, pre_f))
            cur = cur.fromordinal(cur.toordinal() + 1)
    out.sort()
    return out


def build_event_rows(symbol: str, events: list[tuple[_date, float]],
                     table_start: _date) -> list[tuple[_date, float]]:
    """事件 → **事件行表**（base + 变点），不展开逐日。

    逐日表 22M 行只为最后产出 4 万行事件表（内存峰值 ~2GB）——直接按事件构造
    等价结果，量级差 3 个数量级。

    事件行表语义（消费方：``sources.apply_qfq_daily`` 的 as-of backward join、
    ``DataManager._adj_events`` 的相邻行比值）：
      - base 行：table_start 当天的因子 F(table_start)；
      - 变点行：每个事件日一行，值 = 该事件的 fore（事件日当天起生效）。
    """
    if not events:
        return []
    evs = sorted(events)
    # F(table_start)：最后一条 ≤ table_start 的事件；没有则取「首事件之前」的值
    base_val = None
    for d, f in evs:
        if d <= table_start:
            base_val = f
    if base_val is None:
        first_f = evs[0][1]
        base_val = (first_f * (first_f / evs[1][1])
                    if len(evs) >= 2 and evs[1][1] else first_f)
    rows = [(table_start, base_val)]
    prev_val = base_val
    for d, f in evs:
        if d <= table_start:
            continue
        if f != prev_val:
            rows.append((d, f))
            prev_val = f
    return rows


def build_event_table(symbols: list[str], query_start: _date, end: _date,
                      table_start: _date | None = None,
                      progress_every: int = 200,
                      failures_csv: str = "") -> tuple[pl.DataFrame, dict]:
    """全市场回源 → **事件行表**（不展开逐日，内存友好）。

    与「逐日表 → daily_to_events」等价（见 tests 里的一致性用例），但中间
    产物从 22M 行降到几万行。
    """
    bs = _bs()
    _guarded(bs.login, timeout=30)
    stats: dict = {"symbols": len(symbols), "ok": 0, "no_events": 0, "failed": 0,
             "failed_samples": [], "bj_skipped": 0}
    rows_sym: list[str] = []
    rows_date: list[_date] = []
    rows_val: list[float] = []
    failures: list[tuple[str, str]] = []
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
                failures.append((sym, str(exc)[:200]))
                if len(stats["failed_samples"]) < 10:
                    stats["failed_samples"].append(f"{sym}: {str(exc)[:80]}")
                continue
            if not evs:
                stats["no_events"] += 1
                continue
            ev_rows = build_event_rows(sym, evs, table_start or query_start)
            if not ev_rows:
                stats["no_events"] += 1
                continue
            rows_sym.extend([sym] * len(ev_rows))
            rows_date.extend(d for d, _ in ev_rows)
            rows_val.extend(f for _, f in ev_rows)
            stats["ok"] += 1
            if i % progress_every == 0:
                print(f"  [{i}/{len(symbols)}] ok={stats['ok']} 无事件={stats['no_events']} "
                      f"失败={stats['failed']} 用时={time.time()-t0:.0f}s", flush=True)
    finally:
        _guarded(bs.logout, timeout=30)
    if failures and failures_csv:
        _write_failures(failures_csv, failures)
    if not rows_sym:
        return pl.DataFrame(schema={"symbol": pl.String, "trade_date": pl.Date,
                                    "ex_factor": pl.Float64}), stats
    table = pl.DataFrame({
        "symbol": rows_sym, "trade_date": rows_date, "ex_factor": rows_val,
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    table = table.sort(["symbol", "trade_date"])
    stats["rows"] = table.height
    stats["elapsed_s"] = round(time.time() - t0, 1)
    return table, stats


def _write_failures(path: str, failures: list[tuple[str, str]]) -> None:
    """失败标的落 CSV（symbol, 原因, 时间）——不静默丢标的。"""
    import csv
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "reason", "time"])
        for sym, reason in failures:
            w.writerow([sym, reason, _date.today().isoformat()])


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


def daily_to_events(daily: pl.DataFrame, table_start: _date,
                    min_change: float = 0.0) -> pl.DataFrame:
    """逐日因子表 → 事件行表（step function 的稀疏表示）。

    因子是 step function：事件之间恒定 → 只需存「基准行 + 变点」。
    事件行表 = 每个标的一行基准（表起始日的因子）+ 每个变点一行（变点后的因子）。
    消费方（``_adj_events``）取相邻两行的比值即事件乘数 ✓；
    服务端 ``apply_qfq_daily`` 用 as-of backward join 展开 step function ✓。

    22M 行逐日表 → 3.6 万行事件表（600x），客户端加载从 10 GB/70 s 降到 ~5 MB/<1 s。

    min_change：**噪声过滤阈值**（相对变化 |F(t)/F(t-1) - 1|）。按来源分别取：
    - baostock 表（精确因子）噪声底 ~1e-6 → 用 1e-4（保留 0.01% 以上的小额分红）
    - mootdx ETF 表噪声底 ~1e-3（分布断点：296 个 <1e-3，之后直跳 2517）
      → 用 1e-3，否则压缩后相邻行比值 ~4e-4 会被消费端的 1e-4 阈值误判成假事件。
    """
    if daily.is_empty():
        return daily
    srt = daily.sort(["symbol", "trade_date"])
    prev = pl.col("ex_factor").shift(1).over("symbol")
    rel = (pl.col("ex_factor") / prev) - 1.0
    is_change = (rel.abs() > min_change) & prev.is_not_null()
    is_first = prev.is_null()
    changes = srt.filter(is_change | is_first).select(["symbol", "trade_date", "ex_factor"])
    # 基准行：表起始日当天的因子（若起始日恰是变点，其行已在 changes 里）
    base = srt.filter(pl.col("trade_date") == table_start).select(
        ["symbol", "trade_date", "ex_factor"])
    out = pl.concat([base, changes]).unique(subset=["symbol", "trade_date"], keep="last")
    return out.sort(["symbol", "trade_date"])


def _publish(staging: Path, dst: Path, new_symbols: int, old_symbols: int,
             force: bool) -> tuple[bool, str]:
    """把暂存表发布到权威路径：覆盖度护栏 + 备份 + 原子替换。

    护栏（曾经踩过的坑）：直接整表覆盖权威路径时，``--limit 50`` 或一次部分
    网络失败会把 5531 只标的的因子表换成 50 只 → 其余标的**静默失去复权**
    （价格不再折算，回测与实盘口径分裂）。
    """
    if dst.exists():
        if new_symbols < old_symbols and not force:
            return False, (f"新表覆盖 {new_symbols} 只 < 现有 {old_symbols} 只，"
                           f"拒绝覆盖（--force 可强制）")
        bak = dst.with_name(dst.name + ".bak")
        import shutil
        shutil.copy2(dst, bak)
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, dst)
    return True, "已发布"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="",
                    help="输出路径（默认写**暂存**目录 data/adj_factor_baostock_staging/，"
                         "不会覆盖权威表；要发布用 --publish）")
    ap.add_argument("--publish", action="store_true",
                    help="发布到权威表 data/adj_factor_baostock/all.parquet"
                         "（覆盖度护栏 + 备份 + 原子替换）")
    ap.add_argument("--force", action="store_true",
                    help="覆盖度不足时也强制发布（危险）")
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--query-start", default="1990-01-01",
                    help="回源起点：必须早于表起点，才能拿到完整事件史"
                         "（fore 是累计因子，事件日当天起即为该值；缺早期事件会算错起始段）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 只（0=全市场）")
    ap.add_argument("--daily-out", default="", help="额外落一份逐日表（调试用，体积大 600x）")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    start = _date.fromisoformat(args.start)
    end = _date.fromisoformat(args.end)
    query_start = _date.fromisoformat(args.query_start)
    symbols = stock_universe()
    if args.limit:
        symbols = symbols[: args.limit]

    auth_path = Path(DATA_ROOT) / "adj_factor_baostock" / "all.parquet"
    out_path = Path(args.out) if args.out else (
        Path(DATA_ROOT) / "adj_factor_baostock_staging" / "all.parquet")
    failures_csv = str(Path(DATA_ROOT) / "baostock_factor_failures.csv")

    table, stats = build_event_table(symbols, query_start, end, table_start=start,
                                     failures_csv=failures_csv)
    stats["out"] = str(out_path)
    stats["table_start"] = start.isoformat()
    stats["query_start"] = query_start.isoformat()
    if not args.dry_run and not table.is_empty():
        out = out_path
        out.parent.mkdir(parents=True, exist_ok=True)
        # 原子写（tmp + replace）：中断不会留下半个 parquet 在权威路径上
        tmp = out.with_name(out.name + ".tmp")
        table.write_parquet(tmp, compression="zstd")
        os.replace(tmp, out)
        stats["event_rows"] = table.height
        stats["bytes"] = out.stat().st_size
        if args.daily_out:
            Path(args.daily_out).parent.mkdir(parents=True, exist_ok=True)
            table.write_parquet(args.daily_out, compression="zstd")
        if args.publish:
            if stats.get("failed", 0) and not args.force:
                print(json.dumps({"error": "存在回源失败标的，拒绝发布",
                                  "failed": stats["failed"],
                                  "failures_csv": failures_csv},
                                 ensure_ascii=False, indent=2))
                return 2
            old_symbols = 0
            if auth_path.exists():
                old_symbols = pl.read_parquet(auth_path, columns=["symbol"])["symbol"].n_unique()
            ok, msg = _publish(out, auth_path, table["symbol"].n_unique(),
                               old_symbols, args.force)
            stats["published"] = ok
            stats["publish_msg"] = msg
            if not ok:
                print(json.dumps(stats, ensure_ascii=False, indent=2))
                return 3
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
