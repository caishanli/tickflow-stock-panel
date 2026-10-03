#!/usr/bin/env python3
"""模拟盘5确定性核心链优化器：只使用T日可见QFQ特征。

禁止把本脚本网格最优直接冒充未来收益；先按发现段筛选，再看验证段。
"""
from __future__ import annotations

import itertools
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import bt_paper5_v7_202604 as base

OUT = Path("/home/y/.hermes/workspace/stock_rules/reports/paper5_v8_optimizer_QFQ.json")
REPORT = Path("/home/y/.hermes/workspace/stock_rules/reports/模拟盘5_V8优化验证_QFQ.md")


def board_limit_pct(symbol: str) -> float:
    s = str(symbol).upper()
    if s.endswith(".BJ") or s[:2] in {"43", "83", "87", "88", "92"}:
        return 0.30
    if s.startswith("300") or s.startswith("301") or s.startswith("688"):
        return 0.20
    return 0.10


def entry_is_tradeable(symbol: str, prev_close: float, open_px: float,
                       gap_min: float, gap_max: float) -> bool:
    if prev_close <= 0 or open_px <= 0:
        return False
    gap = open_px / prev_close - 1
    limit = board_limit_pct(symbol)
    return gap_min <= gap <= gap_max and gap < limit * 0.99


def signal_passes(row: dict[str, Any], cfg: dict[str, Any]) -> bool:
    mode = cfg.get("mode", "hybrid")
    score_ok = float(row.get("score", -99)) >= float(cfg.get("score_min", 999))
    momo_ok = bool(row.get("momo"))
    if mode == "score" and not score_ok:
        return False
    if mode == "momo" and not momo_ok:
        return False
    if mode == "hybrid" and not (score_ok or momo_ok):
        return False
    if mode == "intersection" and not (score_ok and momo_ok):
        return False
    if momo_ok and mode in ("momo", "intersection"):
        if float(row.get("volr", 0)) < float(cfg.get("volr_min", 0)):
            return False
        if float(row.get("pct", 0)) < float(cfg.get("pct_min", -1)):
            return False
        if float(row.get("close_to_high", 0)) < float(cfg.get("close_to_high_min", 0)):
            return False
    if float(row.get("pos60", 0)) > float(cfg.get("pos60_max", 101)):
        return False
    if float(row.get("rsi6", 0)) > float(cfg.get("rsi6_max", 101)):
        return False
    if cfg.get("trend20_required") and not row.get("trend20"):
        return False
    r3 = float(row.get("ret3", 0))
    if not float(cfg.get("ret3_min", -99)) <= r3 <= float(cfg.get("ret3_max", 99)):
        return False
    return True


def stop_fill_price(buy_price: float, open_price: float, low_price: float,
                    stop_loss: float = 0.07) -> float | None:
    """T+1以后硬止损成交价；跳空时按开盘价，不虚构按止损线成交。"""
    trigger = float(buy_price) * (1.0 - float(stop_loss))
    if float(low_price) > trigger:
        return None
    return min(float(open_price), trigger)


def take_fill_price(buy_price: float, open_price: float, high_price: float,
                    take_profit: float = 0.15) -> float | None:
    """止盈成交价；高开越过止盈线按开盘，否则按止盈线。"""
    trigger = float(buy_price) * (1.0 + float(take_profit))
    if float(high_price) < trigger:
        return None
    return max(float(open_price), trigger)


def bear_reversal_signal(row: dict[str, Any], market_mom: float,
                         min_amount: float = 50_000_000) -> bool:
    """全市场熊市超跌反包；所有输入均为信号日收盘已知。"""
    required = ("ret3", "pct", "open", "close", "volr", "pos60", "amount")
    if market_mom is None or any(row.get(k) is None for k in required):
        return False
    return (
        market_mom <= 0
        and float(row.get("ret3", 0)) < -0.08
        and float(row.get("pct", 0)) > 0.02
        and float(row.get("close", 0)) > float(row.get("open", 0))
        and float(row.get("volr", 0)) > 1.5
        and float(row.get("pos60", 1)) < 0.25
        and float(row.get("amount", 0)) >= min_amount
    )


def book_reversal_features(row: dict[str, Any], prev_row: dict[str, Any]) -> dict[str, Any]:
    """把书本里的“反包质量/开盘气势/止跌量价”翻成T日可见特征。"""
    required = ("open", "high", "low", "close")
    if any(row.get(k) is None for k in required) or prev_row.get("high") is None:
        return {}
    op, hi, lo, cl = (float(row[k]) for k in required)
    span = hi - lo
    if span <= 0:
        return {}
    return {
        "clv": max(0.0, min(1.0, (cl - lo) / span)),
        "body_ratio": max(0.0, min(1.0, abs(cl - op) / span)),
        "lower_shadow_ratio": max(0.0, min(1.0, (min(op, cl) - lo) / span)),
        "reclaims_prev_high": cl > float(prev_row["high"]),
    }


def passes_book_filter(features: dict[str, Any], feature: str, threshold: Any) -> bool:
    """影子A/B只应用一个书本变量；缺值直接排除，禁止静默放行。"""
    value = features.get(feature)
    if value is None:
        return False
    if isinstance(threshold, bool):
        return bool(value) is threshold
    return float(value) >= float(threshold)


def book_quality_score(features: dict[str, Any]) -> float:
    """仅用于同日候选排序，不改变V8准入；权重预注册而非网格寻优。"""
    if not features:
        return 0.0
    score = (
        0.40 * float(features.get("clv", 0.0))
        + 0.30 * float(features.get("body_ratio", 0.0))
        + 0.20 * float(bool(features.get("reclaims_prev_high")))
        + 0.10 * float(features.get("lower_shadow_ratio", 0.0))
    )
    return max(0.0, min(1.0, score))


def enrich_signals(signals, series):
    index = {s: {r["date"]: i for i, r in enumerate(rows)} for s, rows in series.items()}
    out = {}
    for dt, xs in signals.items():
        rows_out = []
        for old in xs:
            x = dict(old)
            sym = x["symbol"]
            i = index[sym].get(dt)
            rows = series[sym]
            if i is None or i < 20:
                continue
            closes = [float(z["close"]) for z in rows]
            ma5 = statistics.mean(closes[i-4:i+1])
            ma20 = statistics.mean(closes[i-19:i+1])
            x["trend20"] = closes[i] > ma20 and ma5 > ma20
            x["ret3"] = closes[i] / closes[i-3] - 1 if i >= 3 else 0
            x["close_to_high"] = closes[i] / float(rows[i]["high"]) if rows[i]["high"] else 0
            x["signal_close"] = closes[i]
            rows_out.append(x)
        out[dt] = rows_out
    return out


@dataclass
class Pos:
    shares: int
    entry: float
    buy_date: str
    days: int = 0
    peak: float = 0.0


def run_variant(dates, series, gua, signals, market, cfg, signal_start=None, signal_end=None):
    idx = {s: {r["date"]: r for r in rows} for s, rows in series.items()}
    cash = base.INIT_CASH
    positions = {}
    pending = []
    cool = {}
    losses = defaultdict(int)
    trades = []
    curve = []

    def sell(sym, px, dt, reason):
        nonlocal cash
        p = positions.pop(sym)
        proceeds = p.shares * px * (1-base.SELL_FEE)
        cost = p.shares * p.entry * (1+base.BUY_FEE)
        cash += proceeds
        trades.append({"symbol": sym, "buy_date": p.buy_date, "sell_date": dt,
                       "entry": p.entry, "exit": px, "ret": proceeds/cost-1,
                       "pnl": proceeds-cost, "reason": reason})
        cool[sym] = dates.index(dt)+3
        if proceeds < cost:
            losses[sym] += 1

    for di, dt in enumerate(dates):
        # 持仓开盘跳空保护
        for sym in list(positions):
            b = idx.get(sym, {}).get(dt)
            if not b:
                continue
            p = positions[sym]
            if b["open"] <= p.entry*(1+cfg["stop"]):
                sell(sym, b["open"], dt, "跳空止损")
            elif cfg.get("take") is not None and b["open"] >= p.entry*(1+cfg["take"]):
                sell(sym, b["open"], dt, "跳空止盈")

        nav_open = cash + sum(p.shares*(idx.get(s, {}).get(dt) or {"open":p.entry})["open"] for s,p in positions.items())
        exposure = nav_open-cash
        opened = 0
        for sig in pending:
            sym = sig["symbol"]
            if opened >= cfg["max_new_per_day"] or sym in positions or len(positions)>=cfg["max_pos"]:
                continue
            if losses[sym] >= 2 or cool.get(sym, -1) >= di:
                continue
            gr = base.active_gua(gua.get(sym, []), dt)
            if not base.gua_allowed(gr)[0]:
                continue
            b = idx.get(sym, {}).get(dt)
            if not b or not entry_is_tradeable(sym, sig["signal_close"], float(b["open"]), cfg["gap_min"], cfg["gap_max"]):
                continue
            budget = min(nav_open*cfg["single"], nav_open*base.MAX_TOTAL-exposure, cash/(1+base.BUY_FEE))
            shares = int(budget/float(b["open"])/100)*100
            if shares < 100:
                continue
            cash -= shares*float(b["open"])*(1+base.BUY_FEE)
            exposure += shares*float(b["open"])
            positions[sym] = Pos(shares, float(b["open"]), dt)
            opened += 1

        for sym in list(positions):
            b = idx.get(sym, {}).get(dt)
            if not b:
                continue
            p = positions[sym]
            if dt == p.buy_date:
                p.peak = max(p.peak, float(b["close"])/p.entry-1)
                continue
            if b["low"] <= p.entry*(1+cfg["stop"]):
                px = b["open"] if b["open"] <= p.entry*(1+cfg["stop"]) else p.entry*(1+cfg["stop"])
                sell(sym, px, dt, "止损")
                continue
            if cfg.get("take") is not None and b["high"] >= p.entry*(1+cfg["take"]):
                px = b["open"] if b["open"] >= p.entry*(1+cfg["take"]) else p.entry*(1+cfg["take"])
                sell(sym, px, dt, "止盈")
                continue
            close_ret = b["close"]/p.entry-1
            p.peak = max(p.peak, close_ret)
            p.days += 1
            trail = cfg.get("trail")
            if trail is not None and p.peak >= cfg.get("trail_start", 0.08) and p.peak-close_ret >= trail:
                sell(sym, b["close"], dt, "移动止盈")
                continue
            ts = cfg.get("time_stop")
            if ts is not None and p.days >= ts and close_ret < 0 and p.peak < 0.03:
                sell(sym, b["close"], dt, "时间止损")

        nav = cash + sum(p.shares*(idx.get(s, {}).get(dt) or {"close":p.entry})["close"] for s,p in positions.items())
        curve.append({"date":dt,"equity":nav})
        if signal_start and dt < signal_start or signal_end and dt > signal_end:
            pending=[]; continue
        rows = [x for x in signals.get(dt, []) if signal_passes(x,cfg)]
        if cfg.get("market_proxy", True) and market.get(dt, {}).get("hard_freeze_proxy"):
            pending=[]; continue
        rows.sort(key=lambda x:(-float(x.get("rank",0)),x["symbol"]))
        pending=rows

    vals=[x["equity"] for x in curve]
    peak=vals[0] if vals else base.INIT_CASH
    dd=0
    for v in vals:
        peak=max(peak,v); dd=min(dd,v/peak-1)
    rets=[x["ret"] for x in trades]
    return {"return": vals[-1]/base.INIT_CASH-1 if vals else 0, "max_dd":dd,
            "trades":len(trades), "win_rate":sum(x>0 for x in rets)/len(rets) if rets else 0,
            "pf":sum(x for x in rets if x>0)/abs(sum(x for x in rets if x<0)) if any(x<0 for x in rets) else None,
            "trade_rows":trades}


def configurations():
    # 每个变量均有宽邻域；不是只测一个“幸运点”。
    for mode in ("momo", "intersection", "score"):
        if mode == "momo":
            products = itertools.product((1.3,1.6,2.0),(0.02,0.03),(0.96,0.98),
                                         (-0.03,-0.01),(0.03,0.05),(-0.07,-0.09),(None,0.15,0.25),(5,8,None))
            for volr,pct,cth,gmin,gmax,stop,take,tstop in products:
                yield {"mode":mode,"score_min":999,"volr_min":volr,"pct_min":pct,"close_to_high_min":cth,
                       "pos60_max":95,"rsi6_max":85,"ret3_min":-0.99,"ret3_max":0.20,
                       "trend20_required":False,"gap_min":gmin,"gap_max":gmax,"stop":stop,"take":take,
                       "time_stop":tstop,"trail":None,"max_pos":10,"single":0.15,"max_new_per_day":3,"market_proxy":True}
        elif mode == "intersection":
            products = itertools.product((0.20,0.35,0.50),(1.3,1.6),(0.02,0.03),(0.96,0.98),
                                         (-0.03,-0.01),(0.03,0.05),(-0.07,-0.09),(None,0.15,0.25),(5,8,None))
            for score,volr,pct,cth,gmin,gmax,stop,take,tstop in products:
                yield {"mode":mode,"score_min":score,"volr_min":volr,"pct_min":pct,"close_to_high_min":cth,
                       "pos60_max":95,"rsi6_max":85,"ret3_min":-0.99,"ret3_max":0.20,
                       "trend20_required":False,"gap_min":gmin,"gap_max":gmax,"stop":stop,"take":take,
                       "time_stop":tstop,"trail":None,"max_pos":10,"single":0.15,"max_new_per_day":3,"market_proxy":True}
        else:
            products = itertools.product((0.35,0.5,0.65),(60,80,95),(False,True),
                                         (-0.03,-0.01),(0.03,0.05),(-0.07,-0.09),(None,0.15,0.25),(5,8,None))
            for score,pos,trend,gmin,gmax,stop,take,tstop in products:
                yield {"mode":mode,"score_min":score,"volr_min":0,"pct_min":-1,"close_to_high_min":0,
                       "pos60_max":pos,"rsi6_max":85,"ret3_min":-0.99,"ret3_max":0.20,
                       "trend20_required":trend,"gap_min":gmin,"gap_max":gmax,"stop":stop,"take":take,
                       "time_stop":tstop,"trail":None,"max_pos":10,"single":0.15,"max_new_per_day":3,"market_proxy":True}


def main():
    gua,_=base.load_gua(); series,dates,end=base.load_price_data(set(gua)); market=base.load_market(dates)
    raw,_,_=base.build_signal_cache(gua,series,dates); signals=enrich_signals(raw,series)
    rows=[]
    for i,cfg in enumerate(configurations()):
        train=run_variant(dates,series,gua,signals,market,cfg,signal_end="2026-07-31")
        valid=run_variant(dates,series,gua,signals,market,cfg,signal_start="2026-08-01")
        full=run_variant(dates,series,gua,signals,market,cfg)
        rows.append({"id":i,"cfg":cfg,"train":{k:v for k,v in train.items() if k!="trade_rows"},
                     "valid":{k:v for k,v in valid.items() if k!="trade_rows"},
                     "full":{k:v for k,v in full.items() if k!="trade_rows"}})
    # 发现段正收益、至少5笔、PF>1；再按验证段排序，避免全样本挑最优。
    eligible=[x for x in rows if x["train"]["trades"]>=5 and x["train"]["return"]>0 and (x["train"]["pf"] or 0)>1]
    eligible.sort(key=lambda x:(x["valid"]["return"],x["full"]["return"]),reverse=True)
    top=eligible[:30]
    robust=[x for x in top if x["valid"]["trades"]>=3 and x["valid"]["return"]>0 and x["full"]["return"]>0]
    payload={"generated_at":str(base.pd.Timestamp.now()),"data_end":end,"grid_count":len(rows),
             "selection":"先用<=2026-07-31发现段筛正收益/PF>1/n>=5，再看>=2026-08-01验证段；验证段不参与首筛",
             "qualified_train":len(eligible),"robust_positive":len(robust),"robust":robust[:10],"top_validation":top[:10]}
    OUT.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    lines=["# 模拟盘5 V8策略优化验证（QFQ）","","> 发现段与验证段分离；网格最优不是未来收益承诺。","",
           "| 项目 | 结果 |","|:---|---:|",f"| 参数组合 | {len(rows)} |",f"| 发现段合格 | {len(eligible)} |",f"| 发现+验证均盈利 | {len(robust)} |","",
           "## 双段均盈利候选","","| ID | 模式 | 发现收益 | 验证收益 | 全段收益 | 回撤 | 交易数 | 核心参数 |","|---:|:---:|---:|---:|---:|---:|---:|:---|"]
    for x in robust[:10]:
        c=x["cfg"]; f=x["full"]
        pars=f"分≥{c['score_min'] if c['mode']=='score' else '—'} 量≥{c['volr_min']} 缺口[{c['gap_min']:.0%},{c['gap_max']:.0%}] 止损{c['stop']:.0%} 止盈{c['take']} 时间{c['time_stop']}"
        lines.append(f"| {x['id']} | {c['mode']} | {x['train']['return']:+.2%} | {x['valid']['return']:+.2%} | {f['return']:+.2%} | {f['max_dd']:+.2%} | {f['trades']} | {pars} |")
    if not robust: lines.append("| — | — | — | — | — | — | — | 没有通过，不改生产策略 |")
    REPORT.write_text("\n".join(lines)+"\n",encoding="utf-8")
    print(json.dumps({k:payload[k] for k in ("grid_count","qualified_train","robust_positive")},ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
