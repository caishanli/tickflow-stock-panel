#!/usr/bin/env python3
"""对比三种名称源（jq/tdx/smart）的回测结果差异。

用法: cd backend && uv run --extra dev python scripts/compare_name_sources.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

# 确保 backend/ 在 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _set_name_source(source: str) -> None:
    from app.quant import db
    db.init_db()
    db.set_quant_setting("sim_strategy_name_source", source)
    print(f"  ✅ 名称源已设置为: {source}")


def _run_backtest(strategy_path: str, params: dict) -> dict:
    from app.quant.rqalpha_bridge import run_jq_backtest
    return run_jq_backtest(strategy_path, params)


def _load_trades(trades_csv: str) -> list[dict]:
    import csv
    rows = []
    with open(trades_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def _load_equity(equity_csv: str) -> list[dict]:
    import csv
    rows = []
    with open(equity_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def _extract_trade_groups(trades: list[dict]) -> dict[str, list[str]]:
    """按交易日期分组，返回 {date: [code, ...]}"""
    groups: dict[str, list[str]] = {}
    for t in trades:
        date = t.get("date", t.get("trade_date", ""))
        code = t.get("code", t.get("symbol", ""))
        if date and code:
            groups.setdefault(date, []).append(code)
    return groups


def _compare_results(name: str, r1: dict, r2: dict, label1: str, label2: str) -> dict:
    """对比两次回测结果，返回差异摘要。"""
    diff = {
        "name": name,
        "source_a": label1,
        "source_b": label2,
    }

    # 1. 净值差异
    eq1 = _load_equity(r1["equity_csv"])
    eq2 = _load_equity(r2["equity_csv"])
    final_eq1 = float(r1.get("final_equity", 0))
    final_eq2 = float(r2.get("final_equity", 0))
    diff["final_equity_a"] = final_eq1
    diff["final_equity_b"] = final_eq2
    diff["equity_diff_pct"] = (final_eq1 - final_eq2) / final_eq2 * 100 if final_eq2 else 0

    # 2. 交易次数
    diff["n_trades_a"] = r1.get("n_trades", 0)
    diff["n_trades_b"] = r2.get("n_trades", 0)

    # 3. 交易日覆盖率（共同交易日占比）
    t1 = _load_trades(r1["trades_csv"])
    t2 = _load_trades(r2["trades_csv"])
    g1 = _extract_trade_groups(t1)
    g2 = _extract_trade_groups(t2)
    dates1 = set(g1.keys())
    dates2 = set(g2.keys())
    common_dates = dates1 & dates2
    union_dates = dates1 | dates2
    diff["trading_days_a"] = len(dates1)
    diff["trading_days_b"] = len(dates2)
    diff["common_trading_days"] = len(common_dates)
    diff["trading_day_overlap_pct"] = len(common_dates) / len(union_dates) * 100 if union_dates else 0

    # 4. 交易标的覆盖率（共同标的占比）
    codes1 = set()
    for codes in g1.values():
        codes1.update(codes)
    codes2 = set()
    for codes in g2.values():
        codes2.update(codes)
    common_codes = codes1 & codes2
    union_codes = codes1 | codes2
    diff["unique_codes_a"] = len(codes1)
    diff["unique_codes_b"] = len(codes2)
    diff["common_codes"] = len(common_codes)
    diff["code_overlap_pct"] = len(common_codes) / len(union_codes) * 100 if union_codes else 0

    # 5. 每日交易标的对比
    daily_match = 0
    daily_total = 0
    for date in common_dates:
        s1 = set(g1[date])
        s2 = set(g2[date])
        daily_total += 1
        if s1 == s2:
            daily_match += 1
    diff["daily_exact_match_pct"] = daily_match / daily_total * 100 if daily_total else 0

    return diff


def main():
    strategy_path = "tests/fixtures/wufu_v52/wufu-v5.2.py"
    params = {
        "start": "2026-04-01",
        "end": "2026-07-16",
        "benchmark": "510300.XSHG",
    }

    sources = ["jq", "tdx", "smart"]
    results = {}

    print("=" * 60)
    print("三种名称源回测对比")
    print("=" * 60)

    for src in sources:
        print(f"\n{'─' * 40}")
        print(f"🔄 回测名称源: {src}")
        print(f"{'─' * 40}")

        _set_name_source(src)
        t0 = time.time()
        result = _run_backtest(strategy_path, params)
        elapsed = time.time() - t0

        if "error" in result:
            print(f"  ❌ 回测失败: {result['error']}")
            continue

        results[src] = result
        print(f"  ⏱️  耗时: {elapsed:.1f}s")
        print(f"  📊 终值: ¥{result['final_equity']:,.2f}")
        print(f"  📈 交易次数: {result['n_trades']}")
        print(f"  🏷️  宇宙大小: {result['universe_size']}")

        m = result.get("metrics", {})
        if m:
            print(f"  📉 年化收益: {m.get('annualized_returns', 'N/A')}")
            print(f"  📉 最大回撤: {m.get('max_drawdown', 'N/A')}")
            print(f"  📉 夏普比率: {m.get('sharpe', 'N/A')}")

    if len(results) < 2:
        print("\n❌ 回测结果不足，无法对比")
        return

    # 两两对比
    print("\n" + "=" * 60)
    print("对比分析")
    print("=" * 60)

    pairs = [
        ("jq", "tdx"),
        ("jq", "smart"),
        ("tdx", "smart"),
    ]

    all_diffs = []
    for s1, s2 in pairs:
        if s1 not in results or s2 not in results:
            continue
        diff = _compare_results(f"{s1}_vs_{s2}", results[s1], results[s2], s1, s2)
        all_diffs.append(diff)

        print(f"\n{'─' * 40}")
        print(f"📊 {s1} vs {s2}")
        print(f"{'─' * 40}")
        print(f"  终值差异: ¥{diff['equity_diff_pct']:+.4f}%")
        print(f"    {s1}: ¥{diff['final_equity_a']:,.2f}  ({diff['n_trades_a']}笔)")
        print(f"    {s2}: ¥{diff['final_equity_b']:,.2f}  ({diff['n_trades_b']}笔)")
        print(f"  交易日覆盖: {diff['common_trading_days']}/{diff['trading_days_a']}天 "
              f"({diff['trading_day_overlap_pct']:.1f}%)")
        print(f"  标的覆盖: {diff['common_codes']}/{max(diff['unique_codes_a'], diff['unique_codes_b'])}只 "
              f"({diff['code_overlap_pct']:.1f}%)")
        print(f"  每日完全匹配: {diff['daily_exact_match_pct']:.1f}%")

    # 汇总
    print(f"\n{'=' * 60}")
    print("汇总")
    print(f"{'=' * 60}")
    for d in all_diffs:
        print(f"  {d['source_a']:>5} vs {d['source_b']:<5}: "
              f"终值差{d['equity_diff_pct']:+.4f}%, "
              f"日重叠{d['trading_day_overlap_pct']:.0f}%, "
              f"标重叠{d['code_overlap_pct']:.0f}%, "
              f"日匹配{d['daily_exact_match_pct']:.0f}%")

    # 保存详细结果
    out_path = Path("data/name_source_comparison.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"diffs": all_diffs, "results": {
            k: {
                "final_equity": v.get("final_equity"),
                "n_trades": v.get("n_trades"),
                "universe_size": v.get("universe_size"),
                "metrics": v.get("metrics"),
            }
            for k, v in results.items()
        }}, f, ensure_ascii=False, indent=2)
    print(f"\n📁 详细结果已保存: {out_path}")


if __name__ == "__main__":
    main()
