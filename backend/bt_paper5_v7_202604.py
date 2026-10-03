#!/usr/bin/env python3
"""模拟盘5 V7 历史日线回放（2026-04-01~最新数据）。

诚实口径：
- 每日只使用当日及此前K线；动态卦期 gua_date<=T<=expiry；最新卦覆盖旧卦。
- V7技术候选使用生产 rule_evaluator 当前评分逻辑，阈值0.10；放量阳线可独立入池。
- T日收盘确认，T+1开盘成交；不使用今天候选倒灌历史。
- 生产无法历史重放的预测LLM/推荐观察/大师临场结论不伪造，报告单列覆盖边界。
"""
from __future__ import annotations

import json
import math
import os
import random
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

START = "2026-04-01"
END_WANTED = "2026-09-07"
INIT_CASH = 500_000.0
BUY_FEE = 0.00025
SELL_FEE = 0.00125
STOP = -0.07
TAKE = 0.15
MAX_TOTAL = 0.95
P5_SINGLE = 0.15
P5_MAX_POS = 18

TF = Path("/home/y/tickflow-stock-panel")
SR = Path("/home/y/.hermes/workspace/stock_rules")
DATA = TF / "data"
QFQ_CACHE = DATA / "cache" / "paper5_v7_qfq_20251101_20260907.json"
OUT_JSON = SR / "reports" / "paper5_v7_backtest_QFQ_20260401_20260907.json"
OUT_MD = SR / "reports" / "模拟盘5_V7回测_QFQ_20260401_20260907.md"
sys.path.insert(0, str(SR))
os.chdir(SR)

from rule_db import get_all_rules  # noqa: E402
from rule_evaluator import compute_features, compute_score, evaluate_rules  # noqa: E402
from rule_screener import KW  # noqa: E402


def sym_of(code: str) -> str | None:
    code = str(code or "").strip()
    if code.startswith("sh"):
        return code[2:] + ".SH"
    if code.startswith("sz"):
        return code[2:] + ".SZ"
    if code.startswith("bj"):
        return code[2:] + ".BJ"
    if len(code) == 6 and code.isdigit():
        if code[0] in "0123":
            return code + ".SZ"
        if code[0] in "456789":
            return code + (".BJ" if code[0] in "489" else ".SH")
    return None


def record_text(r: dict[str, Any]) -> str:
    fields = [r.get("reason"), r.get("raw_text"), r.get("yongshen_state"), r.get("self_derive")]
    gf = r.get("gua_features")
    if isinstance(gf, list):
        fields.extend(gf)
    else:
        fields.append(gf)
    return " ".join(str(x) for x in fields if x)


def load_gua() -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    raw = json.loads((SR / "data/gua_database.json").read_text(encoding="utf-8"))["records"]
    by_sym: dict[str, list[dict[str, Any]]] = defaultdict(list)
    names: dict[str, str] = {}
    for r in raw:
        sym = sym_of(r.get("stock_code", ""))
        gd, ex, direction = str(r.get("gua_date") or ""), str(r.get("expiry_date") or ""), r.get("direction")
        if not sym or not direction or len(gd) < 10 or len(ex) < 10:
            continue
        rr = dict(r)
        rr["_text"] = record_text(r)
        by_sym[sym].append(rr)
        names[sym] = str(r.get("stock_name") or sym)
    for rows in by_sym.values():
        rows.sort(key=lambda x: (str(x["gua_date"]), str(x.get("created_at") or "")))
    return dict(by_sym), names


def active_gua(rows: list[dict[str, Any]], dt: str) -> dict[str, Any] | None:
    active = [r for r in rows if str(r["gua_date"]) <= dt <= str(r["expiry_date"])]
    return active[-1] if active else None


def gua_allowed(r: dict[str, Any] | None) -> tuple[bool, str]:
    if not r:
        return False, "无当时卦期"
    if str(r.get("direction")) != "涨":
        return False, f"卦{r.get('direction')}"
    text = str(r.get("_text") or "")
    # 与当前最终master gate同方向：跌/平、涨卦月破、回头克、死绝、兄弟克财均不放行。
    for kw in ("月破", "回头克", "死绝", "兄弟克财", "兄克财"):
        if kw in text:
            return False, kw
    return True, "涨卦无强空"


def load_price_data(symbols: set[str]) -> tuple[dict[str, list[dict[str, Any]]], list[str], str]:
    parts = sorted((DATA / "kline_daily").glob("date=*/part.parquet"))
    parts = [p for p in parts if "date=2025-11-01" <= p.parent.name <= f"date={END_WANTED}"]
    lf = pl.scan_parquet([str(p) for p in parts]).select(
        "symbol", "date", "open", "high", "low", "close", "volume", "amount"
    ).filter(pl.col("symbol").is_in(sorted(symbols)))
    df = lf.collect().sort(["symbol", "date"])
    if not QFQ_CACHE.exists():
        raise RuntimeError(f"缺少腾讯QFQ缓存: {QFQ_CACHE}")
    qraw = json.loads(QFQ_CACHE.read_text(encoding="utf-8"))
    qmap: dict[str, dict[str, list[float]]] = {}
    for code, item in qraw.items():
        sym = sym_of(code)
        if sym and item.get("rows"):
            qmap[sym] = {str(x[0]): x for x in item["rows"]}
    series: dict[str, list[dict[str, Any]]] = {}
    for sym, sub in df.partition_by("symbol", as_dict=True).items():
        key = sym[0] if isinstance(sym, tuple) else sym
        key = str(key)
        # 禁止用原始价兜底混入；未取得QFQ的标的直接退出本次回测。
        if key not in qmap:
            continue
        rows = sub.to_dicts()
        adjusted = []
        for r in rows:
            r["date"] = str(r["date"])
            qr = qmap[key].get(r["date"])
            if not qr:
                continue
            # 腾讯QFQ行: date, open, close, high, low, volume；amount继续用本地同日值。
            r["open"], r["close"], r["high"], r["low"] = map(float, (qr[1], qr[2], qr[3], qr[4]))
            r["volume"] = float(qr[5] or r["volume"] or 0)
            r["amount"] = float(r["amount"] or 0)
            adjusted.append(r)
        if adjusted:
            series[key] = adjusted
    dates = sorted({r["date"] for rows in series.values() for r in rows if START <= r["date"] <= END_WANTED})
    if not dates:
        raise RuntimeError("回测区间无K线")
    return series, dates, dates[-1]


def load_market(dates: list[str]) -> dict[str, dict[str, float]]:
    parts = sorted((DATA / "kline_daily").glob("date=*/part.parquet"))
    parts = [p for p in parts if "date=2026-03-25" <= p.parent.name <= f"date={dates[-1]}"]
    df = pl.scan_parquet([str(p) for p in parts]).select("symbol", "date", "close", "amount").sort(
        ["symbol", "date"]
    ).with_columns((pl.col("close") / pl.col("close").shift(1).over("symbol") - 1).alias("ret"))
    agg = df.filter((pl.col("amount") >= 30_000_000) & pl.col("ret").is_not_null()).group_by("date").agg(
        pl.col("ret").mean().alias("mean_ret"),
        pl.col("ret").median().alias("median_ret"),
        (pl.col("ret") > 0).mean().alias("breadth"),
        pl.len().alias("n"),
    ).collect().sort("date")
    out = {str(r["date"]): {k: float(r[k]) for k in ("mean_ret", "median_ret", "breadth", "n")} for r in agg.to_dicts()}
    means = [out.get(d, {}).get("mean_ret", 0.0) for d in dates]
    for i, d in enumerate(dates):
        out.setdefault(d, {})["ret3"] = math.prod(1 + x for x in means[max(0, i - 2):i + 1]) - 1
        out[d]["ret20"] = math.prod(1 + x for x in means[max(0, i - 19):i + 1]) - 1
        # 严格代理：极弱宽度/中位跌幅才模拟冰点或多源强空；V7大盘追高阈值为3日+5%。
        out[d]["hard_freeze_proxy"] = float(
            (out[d].get("breadth", 1) < 0.25 and out[d].get("median_ret", 0) < -0.01)
            or (out[d]["ret3"] > 0.05 and out[d].get("mean_ret", 0) > 0)
        )
    return out


def load_benchmark(start: str, end: str) -> tuple[float | None, list[dict[str, Any]]]:
    rows = []
    for p in sorted((DATA / "kline_index_daily").glob("date=*/part.parquet")):
        dt = p.parent.name.replace("date=", "")
        if start <= dt <= end:
            x = pl.read_parquet(p).filter(pl.col("symbol") == "000001.SH")
            if x.height:
                r = x.row(0, named=True)
                rows.append({"date": dt, "close": float(r["close"])})
    if len(rows) < 2:
        return None, rows
    return rows[-1]["close"] / rows[0]["close"] - 1, rows


def frame_until(rows: list[dict[str, Any]], idx: int) -> pd.DataFrame:
    cut = rows[max(0, idx - 249):idx + 1]
    return pd.DataFrame({
        "日期": [x["date"] for x in cut], "开盘": [x["open"] for x in cut],
        "收盘": [x["close"] for x in cut], "最高": [x["high"] for x in cut],
        "最低": [x["low"] for x in cut], "成交量": [x["volume"] for x in cut],
    })


def build_signal_cache(
    gua: dict[str, list[dict[str, Any]]], series: dict[str, list[dict[str, Any]]], dates: list[str]
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[str]], dict[str, int]]:
    rules = get_all_rules()
    quant = [r for r in rules if any(k in f"{r['name']} {r['description']}" for k in KW)]
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    eligible: dict[str, list[str]] = defaultdict(list)
    reject = defaultdict(int)
    date_set = set(dates)
    for sym, grows in gua.items():
        rows = series.get(sym, [])
        for i, bar in enumerate(rows):
            dt = bar["date"]
            if dt not in date_set or i < 60:
                continue
            gr = active_gua(grows, dt)
            ok_gua, why_gua = gua_allowed(gr)
            if not ok_gua:
                reject[why_gua] += 1
                continue
            if bar["amount"] < 30_000_000 or bar["close"] < 2:
                reject["流动性/低价"] += 1
                continue
            feat = compute_features(frame_until(rows, i))
            trig = evaluate_rules(feat, quant)
            score, nb, ns = compute_score(trig)
            prev = rows[i - 1]
            pct = bar["close"] / prev["close"] - 1 if prev["close"] else 0
            vol5 = sum(x["volume"] for x in rows[i - 5:i]) / 5
            volr = bar["volume"] / vol5 if vol5 else 0
            momo = pct > 0.02 and bar["close"] > bar["open"] and volr > 1.3 and pct <= 0.06
            pos60 = feat.get("pos_60") or 0
            rsi6 = feat.get("rsi6") or 0
            if pos60 > 95 and rsi6 > 85:
                reject["极端高位RSI"] += 1
                continue
            eligible[dt].append(sym)
            rank = max(float(score), 1.6 + pct * 10 if momo else -99)
            by_date[dt].append({
                "symbol": sym, "score": float(score), "rank": rank, "momo": momo,
                "pct": pct, "volr": volr, "pos60": pos60, "rsi6": rsi6,
                "gua_date": gr["gua_date"], "expiry": gr["expiry_date"],
            })
    return dict(by_date), dict(eligible), dict(reject)


@dataclass
class Position:
    shares: int
    entry: float
    buy_date: str
    days: int = 0
    peak_ret: float = 0.0


def run_bt(
    dates: list[str], series: dict[str, list[dict[str, Any]]], gua: dict[str, list[dict[str, Any]]],
    signals: dict[str, list[dict[str, Any]]],
    eligible: dict[str, list[str]], market: dict[str, dict[str, float]], threshold: float,
    market_proxy: bool, seed: int | None = None, random_control: bool = False,
    max_pos: int = P5_MAX_POS, single: float = P5_SINGLE,
) -> dict[str, Any]:
    idx = {s: {r["date"]: r for r in rows} for s, rows in series.items()}
    cash = INIT_CASH
    positions: dict[str, Position] = {}
    pending: list[str] = []
    cooldown_until: dict[str, int] = {}
    loss_count: dict[str, int] = defaultdict(int)
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    rng = random.Random(seed)
    source_counts = defaultdict(int)
    blocked_market_days = 0

    def sell(sym: str, px: float, dt: str, reason: str) -> None:
        nonlocal cash
        p = positions.pop(sym)
        proceeds = p.shares * px * (1 - SELL_FEE)
        cash += proceeds
        ret = px * (1 - SELL_FEE) / (p.entry * (1 + BUY_FEE)) - 1
        pnl = proceeds - p.shares * p.entry * (1 + BUY_FEE)
        trades.append({"symbol": sym, "buy_date": p.buy_date, "sell_date": dt, "entry": p.entry,
                       "exit": px, "ret": ret, "pnl": pnl, "reason": reason})
        cooldown_until[sym] = dates.index(dt) + 3
        if pnl < 0:
            loss_count[sym] += 1

    for di, dt in enumerate(dates):
        bars = idx
        # 开盘先处理跳空止损/止盈与前一日信号买入。
        for sym in list(positions):
            b = bars.get(sym, {}).get(dt)
            if not b:
                continue
            p = positions[sym]
            if b["open"] <= p.entry * (1 + STOP):
                sell(sym, b["open"], dt, "跳空止损")
            elif b["open"] >= p.entry * (1 + TAKE):
                sell(sym, b["open"], dt, "跳空止盈")

        nav_open = cash + sum(
            p.shares * (bars.get(s, {}).get(dt) or {"open": p.entry})["open"] for s, p in positions.items()
        )
        exposure = nav_open - cash
        for sym in pending:
            if sym in positions or len(positions) >= max_pos or loss_count[sym] >= 2:
                continue
            # 信号T日有效不等于T+1成交日仍有效；成交前再次用纯日期核验卦期与方向。
            entry_gua = active_gua(gua.get(sym, []), dt)
            if not gua_allowed(entry_gua)[0]:
                continue
            if cooldown_until.get(sym, -1) >= di:
                continue
            b = bars.get(sym, {}).get(dt)
            if not b or b["open"] <= 0:
                continue
            budget = min(nav_open * single, nav_open * MAX_TOTAL - exposure, cash / (1 + BUY_FEE))
            shares = int(budget / b["open"] / 100) * 100
            if shares < 100:
                continue
            cost = shares * b["open"] * (1 + BUY_FEE)
            cash -= cost
            exposure += shares * b["open"]
            positions[sym] = Position(shares, b["open"], dt)

        # 日内止损、止盈、炸板；收盘更新峰值并执行5交易日从未盈利时间止损。
        for sym in list(positions):
            b = bars.get(sym, {}).get(dt)
            if not b:
                continue
            p = positions[sym]
            # A股T+1：当天新买只能盯市，任何止损/止盈/炸板均最早次日执行。
            if dt == p.buy_date:
                close_ret = b["close"] / p.entry - 1
                if close_ret >= 0.05:
                    p.peak_ret = max(p.peak_ret, close_ret)
                continue
            if b["low"] <= p.entry * (1 + STOP):
                px = b["open"] if b["open"] <= p.entry * (1 + STOP) else p.entry * (1 + STOP)
                sell(sym, px, dt, "-7%止损")
                continue
            if b["high"] >= p.entry * (1 + TAKE):
                px = b["open"] if b["open"] >= p.entry * (1 + TAKE) else p.entry * (1 + TAKE)
                sell(sym, px, dt, "+15%止盈")
                continue
            from_hi = b["close"] / b["high"] - 1 if b["high"] else 0
            if b["high"] / p.entry - 1 >= 0.09 and from_hi <= -0.06:
                sell(sym, b["close"], dt, "涨停炸板")
                continue
            # 生产账户只有浮盈达到5%才写peak_ret；轻微日内翻红不算“已改善”。
            close_ret = b["close"] / p.entry - 1
            if close_ret >= 0.05:
                p.peak_ret = max(p.peak_ret, close_ret)
            if dt != p.buy_date:
                p.days += 1
            if p.days >= 5 and b["close"] < p.entry and p.peak_ret <= 0:
                sell(sym, b["close"], dt, "5日不改善时间止损")

        nav_close = cash + sum(
            p.shares * (bars.get(s, {}).get(dt) or {"close": p.entry})["close"] for s, p in positions.items()
        )
        equity_curve.append({"date": dt, "equity": nav_close, "positions": len(positions)})

        # T日收盘产生信号，T+1执行。
        rows = [x for x in signals.get(dt, []) if x["score"] > threshold or x["momo"]]
        if market_proxy and market.get(dt, {}).get("hard_freeze_proxy"):
            pending = []
            if rows:
                blocked_market_days += 1
            continue
        rows.sort(key=lambda x: (-x["rank"], x["symbol"]))
        selected = [x["symbol"] for x in rows]
        if random_control:
            pool = [s for s in eligible.get(dt, []) if s not in selected]
            n = min(len(selected), len(pool))
            selected = rng.sample(pool, n) if n else []
        else:
            for x in rows:
                source_counts["momo" if x["momo"] else "score"] += 1
        pending = selected

    # 末日按收盘盯市，不强平；收益已经在最后净值中。
    equities = [x["equity"] for x in equity_curve]
    peak = equities[0] if equities else INIT_CASH
    max_dd = 0.0
    for v in equities:
        peak = max(peak, v)
        max_dd = min(max_dd, v / peak - 1)
    closed_rets = [t["ret"] for t in trades]
    monthly = {}
    month_first = {}
    month_last = {}
    prev_end = INIT_CASH
    for x in equity_curve:
        m = x["date"][:7]
        month_first.setdefault(m, prev_end)
        month_last[m] = x["equity"]
        prev_end = x["equity"]
    prev = INIT_CASH
    for m in sorted(month_last):
        monthly[m] = month_last[m] / prev - 1
        prev = month_last[m]
    return {
        "final_equity": equities[-1] if equities else INIT_CASH,
        "total_return": (equities[-1] / INIT_CASH - 1) if equities else 0,
        "max_drawdown": max_dd,
        "closed_trades": len(trades),
        "open_positions": len(positions),
        "win_rate": sum(r > 0 for r in closed_rets) / len(closed_rets) if closed_rets else 0,
        "avg_trade": statistics.mean(closed_rets) if closed_rets else 0,
        "median_trade": statistics.median(closed_rets) if closed_rets else 0,
        "profit_factor": (
            sum(r for r in closed_rets if r > 0) / abs(sum(r for r in closed_rets if r < 0))
            if any(r < 0 for r in closed_rets) else None
        ),
        "monthly": monthly,
        "trades": trades,
        "equity_curve": equity_curve,
        "source_counts": dict(source_counts),
        "blocked_market_days": blocked_market_days,
    }


def pct(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:+.2f}%"


def main() -> None:
    gua, names = load_gua()
    series, dates, end = load_price_data(set(gua))
    market = load_market(dates)
    bench, bench_rows = load_benchmark(START, end)
    signals, eligible, rejects = build_signal_cache(gua, series, dates)

    strict = run_bt(dates, series, gua, signals, eligible, market, 0.10, True)
    no_market = run_bt(dates, series, gua, signals, eligible, market, 0.10, False)
    momo_only = run_bt(dates, series, gua, signals, eligible, market, 999.0, True)
    p1 = run_bt(dates, series, gua, signals, eligible, market, 0.35, True, max_pos=12, single=0.15)
    sweeps = {str(t): run_bt(dates, series, gua, signals, eligible, market, t, True) for t in (0.0, 0.1, 0.2, 0.35, 0.5)}
    random_runs = [run_bt(dates, series, gua, signals, eligible, market, 0.10, True, seed=i,
                          random_control=True) for i in range(200)]
    random_rets = [x["total_return"] for x in random_runs]
    rand = {
        "mean": statistics.mean(random_rets), "median": statistics.median(random_rets),
        "p10": sorted(random_rets)[19], "p90": sorted(random_rets)[179],
        "beat_rate": sum(strict["total_return"] > x for x in random_rets) / len(random_rets),
    }
    active_days = sum(bool(eligible.get(d)) for d in dates)
    signal_days = sum(bool(signals.get(d)) for d in dates)
    earliest = min((r["gua_date"] for rows in gua.values() for r in rows), default=None)
    result = {
        "generated_at": str(pd.Timestamp.now()), "data_start": dates[0], "data_end": end,
        "initial_cash": INIT_CASH, "rules": "模拟盘5 进攻V7·扩池高利用率（可历史重放确定性子集）",
        "coverage": {"trade_days": len(dates), "earliest_directional_gua": earliest,
                     "active_pool_days": active_days, "signal_days": signal_days,
                     "symbols_with_gua": len(gua), "symbols_with_prices": len(series),
                     "unreplayable": ["盘前LLM预测方向/置信度", "推荐观察级历史记录", "大师临场会诊/深度核查",
                                      "4源强空原始历史快照", "分钟内精确触发与成交价", "历史时点Kelly训练样本"]},
        "strict": strict, "without_market_proxy": no_market, "momo_only_no_rule_weight": momo_only,
        "paper1_threshold_control": p1,
        "threshold_sweep": sweeps, "random_control_200": rand, "benchmark_sh000001": bench,
        "reject_counts": rejects,
    }
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    months = sorted(strict["monthly"])
    momo_pf = f"{momo_only['profit_factor']:.2f}" if momo_only["profit_factor"] is not None else "—"
    lines = [
        "# 模拟盘5 V7 可重放核心规则回测（非完整直播复刻）", "",
        "> ⚠️ 结论边界：缺少逐日直播快照的LLM预测、推荐观察、临场会诊与历史Kelly，不能声称这是完整V7的精确净值；这是能严格按日期重放的核心交易链压力测试。", "",
        f"> 数据区间：{dates[0]}～{end}｜初始资金：¥{INIT_CASH:,.0f}｜动态卦期｜T+1开盘｜生产费率0.025%买+0.125%卖", "",
        "## 核心结果", "",
        "| 口径 | 期末净值 | 总收益 | 最大回撤 | 平仓笔数 | 胜率 | 均笔 | 盈亏因子 |",
        "|:---|---:|---:|---:|---:|---:|---:|---:|",
        f"| **V7严格主结果（市场硬闸代理）** | **¥{strict['final_equity']:,.0f}** | **{pct(strict['total_return'])}** | {pct(strict['max_drawdown'])} | {strict['closed_trades']} | {strict['win_rate']*100:.1f}% | {pct(strict['avg_trade'])} | {strict['profit_factor']:.2f} |",
        f"| V7不使用不可还原市场顶闸 | ¥{no_market['final_equity']:,.0f} | {pct(no_market['total_return'])} | {pct(no_market['max_drawdown'])} | {no_market['closed_trades']} | {no_market['win_rate']*100:.1f}% | {pct(no_market['avg_trade'])} | {no_market['profit_factor']:.2f} |",
        f"| 纯放量阳线（不读当前规则绩效权重） | ¥{momo_only['final_equity']:,.0f} | {pct(momo_only['total_return'])} | {pct(momo_only['max_drawdown'])} | {momo_only['closed_trades']} | {momo_only['win_rate']*100:.1f}% | {pct(momo_only['avg_trade'])} | {momo_pf} |",
        f"| 盘1阈值对照（0.35） | ¥{p1['final_equity']:,.0f} | {pct(p1['total_return'])} | {pct(p1['max_drawdown'])} | {p1['closed_trades']} | {p1['win_rate']*100:.1f}% | {pct(p1['avg_trade'])} | {p1['profit_factor']:.2f} |",
        f"| 同期上证指数 | — | {pct(bench)} | — | — | — | — | — |", "",
        "## 同日动态池随机对照（200次）", "",
        "| V7收益 | 随机均值 | 随机中位 | 随机P10～P90 | V7跑赢随机比例 |",
        "|---:|---:|---:|---:|---:|",
        f"| {pct(strict['total_return'])} | {pct(rand['mean'])} | {pct(rand['median'])} | {pct(rand['p10'])}～{pct(rand['p90'])} | {rand['beat_rate']*100:.1f}% |", "",
        "## 阈值邻域敏感性", "",
        "| 技术分阈值 | 总收益 | 最大回撤 | 平仓笔数 | 胜率 |",
        "|---:|---:|---:|---:|---:|",
    ]
    for t in (0.0, 0.1, 0.2, 0.35, 0.5):
        r = sweeps[str(t)]
        lines.append(f"| {t:.2f} | {pct(r['total_return'])} | {pct(r['max_drawdown'])} | {r['closed_trades']} | {r['win_rate']*100:.1f}% |")
    lines += ["", "## 月度收益（主结果）", "", "| 月份 | 收益 |", "|:---:|---:|"]
    for m in months:
        lines.append(f"| {m} | {pct(strict['monthly'][m])} |")
    lines += [
        "", "## 数据覆盖与诚实边界", "",
        "| 项目 | 数值 |", "|:---|---:|",
        f"| 交易日 | {len(dates)} |", f"| 有真实动态卦池的交易日 | {active_days} |",
        f"| 产生技术信号的交易日 | {signal_days} |", f"| 最早方向卦 | {earliest} |",
        f"| 市场代理硬闸拦截有信号日 | {strict['blocked_market_days']} |", "",
        "以下直播字段没有4月至今逐日快照，因此没有伪造：盘前LLM预测、推荐观察级、大师临场会诊、4源强空原始快照、分钟精确成交、历史时点Kelly。主结果用可重放规则，并把市场硬闸开/关都列出；当前规则评分权重冻结于2026-09-07，属于参数回看，不是完全走样本外成绩。", "",
        f"> JSON明细：`{OUT_JSON}`",
    ]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({
        "report": str(OUT_MD), "json": str(OUT_JSON), "data_end": end,
        "strict_return": strict["total_return"], "strict_final": strict["final_equity"],
        "strict_dd": strict["max_drawdown"], "strict_trades": strict["closed_trades"],
        "strict_win": strict["win_rate"], "no_market_return": no_market["total_return"],
        "momo_only_return": momo_only["total_return"],
        "p1_return": p1["total_return"], "benchmark": bench, "random": rand,
        "active_days": active_days, "signal_days": signal_days,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
