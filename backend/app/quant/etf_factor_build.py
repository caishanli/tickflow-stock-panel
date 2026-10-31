"""ETF 前复权因子表构建：**mootdx xdxr 原始事件 + 本机原始价**。

为什么不用别的源（都实测过，别再试）
------------------------------------
* **baostock 的 ETF 前复权序列是错的**：`adjustflag=2` 的 qfq/raw 比值会在
  无价格跳空的日期凭空跳变（实测 510880.SH 2009-03-24 原始价 −0.15%、qfq
  却 −44%；510300.SH 2012-12-18 原始价 −1.5%、qfq −21%）。拿它反推因子
  等于把假跳变写进因子表。**不要**再用 baostock qfq 构建 ETF 因子。
* **TickFlow `ex_factors`** 是设计上的权威源，但 Free 档无权限
  （`PermissionError 无除权因子查询权限`），现有表只有 2025-06-30 起的快照。
* **xdxr**（通达信除权除息）覆盖 ETF 全历史（368 只 ETF 有事件），且与
  TickFlow 已知因子在重叠区间一致（15 只抽样：13 只 ≤1%、2 只 ≤1.7%）——
  本模块用它。

算法
----
1. 逐标的取 xdxr 事件（category 1 = 分红/送转/配股，11 = 缩股/份额折算）；
2. 用**本机原始价**（`kline_etf_daily`，已是 raw）算除权因子：
   ``factor = ex_price / prev_close``，并把 ``dates < ex_date`` 的价格乘上它
   （与 `apply_qfq` 的 as-of 语义一致，最新锚定 = 1.0）；
3. 复用 `mootdx_service._merge_cliff_factors` 兜底**份额折算**类悬崖
   （xdxr 不含此类事件，561980 案例）；
4. 压缩成**事件行**（step function：只在因子变化处留一行）；
5. 结构校验：锚点必须 = 1.0、必须非递减、大 step（>5%）必须在原始价上
   有对应的跳空 —— 不满足的 step 丢弃并计数（宁可少调，不写假跳变）。

输出：``data/adj_factor_etf/all.parquet``（symbol=tushare 码 / trade_date / ex_factor）。
默认 dry-run；写盘前备份旧表到 ``data/.daily_basis_backup/adj_factor_etf/``。
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
from datetime import date
from pathlib import Path

import pandas as pd
import polars as pl

DATA = Path(__file__).resolve().parents[3] / "data"
OUT_PATH = DATA / "adj_factor_etf" / "all.parquet"
BACKUP_DIR = DATA / ".daily_basis_backup" / "adj_factor_etf"
XDXR_PATH = DATA / "adj_factor_etf" / "xdxr_events.parquet"

SINCE_YEAR = 2005
STEP_EPS = 1e-6          # 因子变化小于此值视为同一 step
CLIFF_THRESHOLD = 0.30   # 份额折算悬崖阈值：20cm ETF 合法涨跌可到 20.03%，不能用 0.2
STEP_EVIDENCE = 0.25     # step 必须在原始价上找到 ≥25% 步长的对应跳空（±1 日窗口内）


def _excess_returns(closes: dict[str, pd.Series]) -> dict[date, float]:
    """每个交易日的 **ETF 收益中位数**（当作"大盘"因子，用于剔除市场波动）。"""
    per_day: dict[date, list[float]] = {}
    for ser in closes.values():
        vals = ser.dropna()
        if len(vals) < 2:
            continue
        rets = vals.pct_change().dropna()
        for ts, r in rets.items():
            per_day.setdefault(ts.date(), []).append(float(r))
    return {d: statistics.median(v) for d, v in per_day.items() if v}


def compress_steps(daily: list[tuple[date, float]],
                   eps: float = STEP_EPS) -> list[tuple[date, float]]:
    """逐日因子序列 → 事件行（只在变化处保留，含首行）。"""
    out: list[tuple[date, float]] = []
    prev = None
    for d, f in daily:
        if prev is None or abs(f - prev) > eps:
            out.append((d, round(float(f), 8)))
            prev = f
    return out


def steps_from_daily(daily: list[tuple[date, float]]) -> list[tuple[date, float]]:
    """逐日因子序列 → ``[(变点日, 该点的因子比值)]``。

    因子是累积量（末值 = 1.0），每个变点把**它之前**的所有日期乘上该比值。
    """
    steps: list[tuple[date, float]] = []
    prev = None
    for d, f in daily:
        if prev is None:
            prev = f
            continue
        if abs(f - prev) > STEP_EPS:
            steps.append((d, f / prev))
            prev = f
    return steps


def rebuild_factors(dates: list[date], steps: list[tuple[date, float]]) -> list[tuple[date, float]]:
    """按保留的 step 重算逐日因子（后缀乘积）——**末值恒为 1.0**。

    丢弃某个 step 等价于"该事件不参与复权"，其余事件的乘积自动重算；
    若直接删行会导致其后所有日期残留旧因子（实测造成 24 只末值 ≠1.0）。
    """
    if not steps:
        return [(d, 1.0) for d in dates]
    srt = sorted(steps)
    # 因子(t) = 1 / Π_{事件日 > t} r  —— 事件只会把**更早**的日期往下调，
    # 所以越往前越小；末值（无事件在后）= 1.0。
    f = 1.0
    for _, r in srt:
        f /= r
    out: list[tuple[date, float]] = []
    j = 0
    for d in dates:
        while j < len(srt) and srt[j][0] <= d:
            f *= srt[j][1]
            j += 1
        out.append((d, f))
    return out


def validate_events(events: list[tuple[date, float]],
                    closes: dict[date, float],
                    market: dict[date, float] | None = None) -> tuple[list[tuple[date, float]], list[str]]:
    """校验每个因子 step 是否能在原始价上找到**对应跳空**；返回 (保留, 问题)。

    判据（复权后必须连续 ⇒ 因子变化与价格跳空**必然反向**）：
      对 step ``s = f_i/f_{i-1} - 1``，用该日原始价收益减去当日 ETF 收益中位数
      （当"大盘"因子，剔除板块/市场波动）得到 ``excess``；在 ±1 日窗口内取
      与 ``-s`` 同向的最大证据，要求 ``evidence ≥ 0.25·|s|``，否则视为凭空
      跳变 → 丢弃该 step。

    这样既能放过"分红日大盘也涨"的正常情形，也能拦住日期错位/凭证错配的
    假事件（159971.SZ 的 +20.03% 涨停日曾被误当份额折算，即此类）。
    """
    problems: list[str] = []
    market = market or {}
    dates = sorted(closes)
    idx_of = {d: i for i, d in enumerate(dates)}

    def _excess(at: int) -> float | None:
        if at <= 0 or at >= len(dates):
            return None
        p0, p1 = closes[dates[at - 1]], closes[dates[at]]
        if p0 <= 0:
            return None
        return (p1 / p0 - 1.0) - market.get(dates[at], 0.0)

    kept: list[tuple[date, float]] = []
    for d, ratio in events:
        step = ratio - 1.0
        if abs(step) > 1e-9:
            at = idx_of.get(d)
            evidence = 0.0
            sign = 1.0 if step > 0 else -1.0
            if at is not None:
                for w in (-1, 0, 1):
                    ex = _excess(at + w)
                    if ex is not None:
                        evidence = max(evidence, -sign * ex)
            # 复权连续 ⇒ 价格跳空的**期望幅度** r = |s|/(1+|s|)（1:12 拆分时
            # 价格只跌 92%，而因子 step 是 +1100% —— 不能拿 |s| 当门槛）
            expected = abs(step) / (1.0 + abs(step))
            if evidence < STEP_EVIDENCE * expected:
                problems.append(
                    f"step_without_price_move:{d}:{step:+.4f}:evidence{evidence:+.4f}")
                continue
        kept.append((d, ratio))
    return kept, problems


def factor_events_for_symbol(symbol: str, closes: pd.Series,
                             xdxr_rows: list[dict],
                             market: dict[date, float] | None = None
                             ) -> tuple[list[tuple[date, float]], list[str]]:
    """单只 ETF：xdxr 事件 + 原始价 → 校验后的因子事件行。"""
    from app.services.mootdx_service import _merge_cliff_factors, _symbol_factor_frame

    if closes is None or len(closes) < 2:
        return [], ["no_price"]
    fr = _symbol_factor_frame(symbol, closes, xdxr_rows, since_year=SINCE_YEAR)
    fr = _merge_cliff_factors(symbol, closes, fr, threshold=CLIFF_THRESHOLD)
    if fr is None or fr.is_empty():
        return [], []
    daily = [(date.fromisoformat(r["trade_date"]), float(r["ex_factor"]))
             for r in fr.iter_rows(named=True)]
    steps = steps_from_daily(daily)
    cmap = {d.date(): float(v) for d, v in closes.dropna().items()}
    kept, problems = validate_events(steps, cmap, market)
    if not kept:
        return [], problems
    rebuilt = rebuild_factors(sorted(cmap), kept)
    # 全 1.0 = 无需复权（丢弃 step 后可能如此），不写空表
    if all(abs(f - 1.0) <= 1e-9 for _, f in rebuilt):
        return [], problems
    return compress_steps(rebuilt), problems


def load_etf_closes() -> dict[str, pd.Series]:
    """一次性读全 ETF 日线 close（原始价），按标的分组。"""
    import glob

    series: dict[str, dict[pd.Timestamp, float]] = {}
    for f in sorted(glob.glob(str(DATA / "kline_etf_daily" / "date=*" / "part.parquet"))):
        day = f.split("date=")[1][:10]
        df = pl.read_parquet(f, columns=["symbol", "close"])
        ts = pd.Timestamp(day)
        for r in df.iter_rows(named=True):
            if r["close"] is None:
                continue
            series.setdefault(str(r["symbol"]), {})[ts] = float(r["close"])
    return {k: pd.Series(v).sort_index() for k, v in series.items()}


def load_xdxr() -> dict[str, list[dict]]:
    df = pl.read_parquet(XDXR_PATH)
    out: dict[str, list[dict]] = {}
    for r in df.iter_rows(named=True):
        out.setdefault(str(r["symbol"]), []).append(dict(r))
    return out


def build(*, apply: bool = False, out_path: Path = OUT_PATH,
          symbols: list[str] | None = None) -> dict:
    """构建全量 ETF 因子事件表。返回统计。"""
    closes = load_etf_closes()
    xdxr = load_xdxr()
    market = _excess_returns(closes)
    if symbols is None:
        inst = DATA / "instruments_etf" / "instruments_etf.parquet"
        symbols = sorted({str(s) for s in pl.read_parquet(inst, columns=["symbol"])["symbol"].to_list()})
    stats: dict = {
        "symbols": len(symbols), "with_events": 0, "event_rows": 0,
        "no_events": 0, "no_price": 0, "problems": {}, "problem_samples": [],
        "apply": apply, "out_path": str(out_path), "backup": None,
    }
    frames = []
    for sym in symbols:
        code = sym.split(".")[0]
        rows = xdxr.get(code, [])
        ser = closes.get(sym)
        if ser is None or ser.empty:
            stats["no_price"] += 1
            continue
        events, problems = factor_events_for_symbol(sym, ser, rows, market)
        for p in problems:
            key = p.split(":")[0]
            stats["problems"][key] = stats["problems"].get(key, 0) + 1
            if len(stats["problem_samples"]) < 10:
                stats["problem_samples"].append({"symbol": sym, "problem": p})
        if not events:
            stats["no_events"] += 1
            continue
        stats["with_events"] += 1
        stats["event_rows"] += len(events)
        frames.append(pl.DataFrame({
            "symbol": [sym] * len(events),
            "trade_date": [d for d, _ in events],
            "ex_factor": [f for _, f in events],
        }))
    if not frames:
        stats["error"] = "no events built"
        return stats
    table = pl.concat(frames).with_columns(
        pl.col("trade_date").cast(pl.Date), pl.col("ex_factor").cast(pl.Float64)
    ).sort(["symbol", "trade_date"])
    stats["rows_total"] = table.height
    stats["min_date"] = str(table["trade_date"].min())
    stats["max_date"] = str(table["trade_date"].max())
    if not apply:
        return stats
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        backup = BACKUP_DIR / f"all_{date.today().isoformat()}.parquet"
        if not backup.exists():
            backup.write_bytes(out_path.read_bytes())
        stats["backup"] = str(backup)
    tmp = out_path.with_suffix(".parquet.tmp")
    table.write_parquet(tmp)
    os.replace(tmp, out_path)
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="用 xdxr 事件构建 ETF 前复权因子事件表")
    ap.add_argument("--apply", action="store_true", help="真正写盘（否则 dry-run）")
    ap.add_argument("--symbols", default="", help="逗号分隔的标的（调试用）")
    args = ap.parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] or None
    stats = build(apply=args.apply, symbols=symbols)
    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
    return 0 if stats.get("with_events") else 1


if __name__ == "__main__":
    raise SystemExit(main())
