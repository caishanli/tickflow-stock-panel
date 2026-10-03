#!/usr/bin/env python3
"""V8书本规则影子A/B：冻结生产，只做预注册的三个单变量实验。"""
from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import p5_bear_qfq_backtest as core
import polars as pl
from p5_strategy_optimizer import book_reversal_features

OUT = Path('/home/y/.hermes/workspace/stock_rules/reports/p5_v8_book_shadow.json')
MD = Path('/home/y/.hermes/workspace/stock_rules/reports/模拟盘5_V8书本规则影子实验.md')
CUT = '2026-04-30'

# 只预注册三个能由T日日线确定的主假设，避免28笔上无限找参数。
EXPERIMENTS = [
    {"id": "clv", "name": "收盘位置CLV", "source": "麻道明·烂板质量；谢佳颖·突破真假", "threshold": 0.75},
    {"id": "body_ratio", "name": "反包实体占比", "source": "麻道明·首板突破质量／强势启动", "threshold": 0.50},
    {"id": "reclaims_prev_high", "name": "收复前一日高点", "source": "谢佳颖·突破真假；利弗莫尔／卡沃尔·关键点", "threshold": True},
]


def enrich_book_features(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    previous: dict[str, dict[str, Any]] = {}
    out = []
    for original in sorted(rows, key=lambda x: (str(x['symbol']), str(x['date']))):
        row = dict(original)
        row['book'] = book_reversal_features(row, previous.get(str(row['symbol']), {}))
        previous[str(row['symbol'])] = row
        out.append(row)
    return out


def transform_pools(pools, feature: str, descending: bool = True, threshold: Any = None):
    transformed = defaultdict(list)
    for dt, rows in pools.items():
        kept = []
        for original in rows:
            row = dict(original)
            value = row.get('book', {}).get(feature)
            if threshold is not None:
                if value is None:
                    continue
                if isinstance(threshold, bool):
                    if bool(value) is not threshold:
                        continue
                elif float(value) < float(threshold):
                    continue
            kept.append(row)
        def key(row):
            value = row.get('book', {}).get(feature)
            if value is None:
                return (1, 0.0, str(row['symbol']))
            numeric = float(bool(value)) if isinstance(value, bool) else float(value)
            return (0, -numeric if descending else numeric, str(row['symbol']))
        transformed[dt] = sorted(kept, key=key)
    return transformed


def as_ranked_core_pool(pools, feature: str, descending: bool, threshold: Any = None):
    """core.bt按volr降序取每日前三；影子副本只替换排序键，不改准入信号。"""
    ranked = transform_pools(pools, feature, descending, threshold)
    out = defaultdict(list)
    for dt, rows in ranked.items():
        n = len(rows)
        for i, original in enumerate(rows):
            row = dict(original)
            row['_original_volr'] = row.get('volr')
            row['volr'] = float(n - i)
            out[dt].append(row)
    return out


def strip(result):
    return {k: result[k] for k in ('return', 'trades', 'win_rate', 'max_dd', 'avg')}


def run_three_windows(qdf, pools, prepared):
    kwargs = dict(hold=2, max_pos=5, max_day=3, prepared=prepared)
    return {
        'discovery': strip(core.bt(qdf, pools, start=core.START, end=CUT, **kwargs)),
        'validation': strip(core.bt(qdf, pools, start='2026-05-01', end=core.END, **kwargs)),
        'full': strip(core.bt(qdf, pools, **kwargs)),
    }


def percentile(values, x):
    return 100.0 * sum(v <= x for v in values) / len(values) if values else math.nan


def main():
    local, symbols, _ = core.load_local()
    cache = core.fetch(symbols)
    ok = [s for s in symbols if cache.get(core.code_of(s), {}).get('rows')]
    qdf = core.build_qfq(local, ok, cache)
    mm = core.market_momentum(local)
    signals, controls = core.make_pools(qdf, mm)
    prepared = core.prepare_bt(qdf)

    # 只把227个信号行转成Python对象，避免把全市场QFQ全量to_dicts顶爆网关内存。
    signal_keys = [(r['symbol'], str(r['date'])) for rows in signals.values() for r in rows]
    keys = pl.DataFrame({'symbol': [x[0] for x in signal_keys], 'date_key': [x[1] for x in signal_keys]})
    small = (
        qdf.with_columns([
            pl.col('high').shift(1).over('symbol').alias('prev_high'),
            pl.col('date').cast(pl.String).alias('date_key'),
        ])
        .join(keys, on=['symbol', 'date_key'], how='inner')
        .select('symbol', 'date_key', 'open', 'high', 'low', 'close', 'prev_high')
    )
    books = {}
    for row in small.to_dicts():
        books[(row['symbol'], row['date_key'])] = book_reversal_features(
            row, {'high': row.get('prev_high')}
        )
    annotated = defaultdict(list)
    for dt, rows in signals.items():
        for original in rows:
            row = dict(original)
            row['book'] = books.get((row['symbol'], str(row['date'])), {})
            annotated[dt].append(row)

    baseline = run_three_windows(qdf, annotated, prepared)
    results = []
    for exp in EXPERIMENTS:
        forward = as_ranked_core_pool(annotated, exp['id'], True)
        reverse = as_ranked_core_pool(annotated, exp['id'], False)
        filtered = as_ranked_core_pool(annotated, exp['id'], True, exp['threshold'])
        results.append({
            **exp,
            'forward_rank': run_three_windows(qdf, forward, prepared),
            'reverse_rank': run_three_windows(qdf, reverse, prepared),
            'hard_filter_diagnostic': run_three_windows(qdf, filtered, prepared),
        })

    # 完整随机分布用于描述基线位置；主假设仍只有上面三个。
    random_returns = [core.bt(qdf, controls, 2, 5, 3, seed=i, prepared=prepared)['return'] for i in range(10_000)]
    random_sorted = sorted(random_returns)
    random_summary = {
        'n': len(random_returns),
        'mean': statistics.mean(random_returns),
        'median': statistics.median(random_returns),
        'p05': random_sorted[499],
        'p95': random_sorted[9499],
        'baseline_percentile': percentile(random_returns, baseline['full']['return']),
    }
    payload = {
        'status': 'shadow_only_production_v8_frozen',
        'data_end': core.END,
        'hypotheses': len(EXPERIMENTS),
        'qfq_coverage': {'ok': len(ok), 'wide_symbols': len(symbols)},
        'baseline': baseline,
        'experiments': results,
        'random_control': random_summary,
        'promotion_rule': '历史只作淘汰；2026-09-08后累计>=40笔且>=20信号日再初审，>=60笔且>=30信号日、覆盖两种市场状态才可申请V9灰度。',
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    lines = [
        '# 模拟盘5 V8书本规则影子实验', '',
        '> 生产V8参数冻结。本报告把书本定性原则转成三个预注册单变量；历史结果只用于淘汰，不直接上线。', '',
        '| 口径 | 数值 |', '|:---|---:|',
        f"| 腾讯QFQ覆盖 | {len(ok)}/{len(symbols)} |",
        f"| V8基线全段 | {baseline['full']['return']:+.2%} |",
        f"| V8基线交易数 | {baseline['full']['trades']} |",
        f"| 10,000组随机均值 | {random_summary['mean']:+.2%} |",
        f"| 随机5%～95%区间 | {random_summary['p05']:+.2%} ～ {random_summary['p95']:+.2%} |",
        f"| V8位于随机分布百分位 | {random_summary['baseline_percentile']:.2f}% |", '',
        '## 三个预注册单变量', '',
        '| 变量 | 书本来源 | 正向排序全段 | 反向排序全段 | 硬过滤全段 | 保留交易 | 发现段 | 验证段 |',
        '|:---|:---|---:|---:|---:|---:|---:|---:|',
    ]
    for x in results:
        fwd=x['forward_rank']; rev=x['reverse_rank']; flt=x['hard_filter_diagnostic']
        lines.append(f"| {x['name']} | {x['source']} | {fwd['full']['return']:+.2%} | {rev['full']['return']:+.2%} | {flt['full']['return']:+.2%} | {flt['full']['trades']} | {flt['discovery']['return']:+.2%} | {flt['validation']['return']:+.2%} |")
    lines += ['', '## 晋升纪律', '',
              '| 阶段 | 硬条件 |', '|:---|:---|',
              '| 当前 | 仅影子记录，严禁回写V8生产阈值 |',
              '| 初审 | 2026-09-08后≥40笔、≥20个信号日；只比较预注册三项 |',
              '| 灰度申请 | ≥60笔、≥30个信号日、覆盖至少两种市场状态；簇Bootstrap与Holm校正通过 |',
              '| 淘汰 | 正向排序不优于反向、依赖单一日期簇、或硬过滤保留不足20笔 |']
    MD.write_text('\n'.join(lines)+'\n', encoding='utf-8')
    print(json.dumps({'baseline': baseline['full'], 'experiments': [{x['id']: {'forward': x['forward_rank']['full'], 'reverse': x['reverse_rank']['full'], 'filter': x['hard_filter_diagnostic']['full']}} for x in results], 'random': random_summary}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
