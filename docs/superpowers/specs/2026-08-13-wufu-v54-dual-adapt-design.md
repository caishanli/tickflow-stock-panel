# wufu v5.4 双持仓自适应策略（wufu-v5.4-dual-adapt）

- 日期：2026-08-13
- 分支：`explore/dual-position-v54`
- 文件：`backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.py`
- 目标：在 wufu v5.4（单持仓动量轮动）基础上做**双持仓**，收益/夏普/最大回撤全面超越 v5.4。

## 结论（修正引擎后，各窗口独立对比）

| 窗口 | 指标 | v5.4 基线 | dual-adapt | 对比 |
|---|---|---|---|---|
| 全窗口 04-01~08-11 | 收益 | +26.45% | **+27.10%** | **+0.65pp** |
| | 夏普 | 1.87 | **2.18** | **+0.31** |
| | maxDD | -16.53% | **-12.02%** | **+4.5pp** |
| 对齐 04-01~07-16 | 收益 | +20.87% | +20.62% | -0.25pp |
| | 夏普 | 1.88 | **2.23** | **+0.35** |
| | maxDD | -16.53% | **-12.02%** | **+4.5pp** |
| 模拟 07-10~08-11 | 收益 | +4.63% | **+5.38%** | **+0.75pp** |
| | 夏普 | 1.51 | **1.70** | **+0.19** |
| | maxDD | -5.83% | -5.89% | 持平 |

- **全窗口收益/夏普/回撤三优**；模拟盘（前向）收益/夏普优；对齐窗口收益基本打平（-0.25pp）、夏普/回撤显著优。

## 策略逻辑

在 wufu v5.4 的动量/过滤/止损框架上，把 `holdings_num` 从 1 改为 2，并重写第 4 步选股：

- **slot0 = 全池动量第一**（大权重）。保留粘性：现有 top10 持仓且动量 ≥ 第一×0.9 时保留（与 v5.4 候选池口径一致，降换手）。
- **slot1 = 另一资产大类动量第一**（全球/海外 vs 大A/港股），要求动量 ≥ 0.3（`cross_slot1_floor`）。保留粘性：现有同类持仓 ≥ 类首×0.85 时保留。
- **自适应权重**：`w0 = m0/(m0+m1)`，clamp 到 [0.5, 0.85]。slot1 越弱仓位越小（弱腿仅小仓对冲，不摊薄收益）；slot1 无候选时退化为单持仓（全仓 slot0）。
- 买入分配按 `target_weights` 槽位目标市值下单（最后一笔用剩余现金）。

动机：同一板块的 top1/top2 高度相关（同涨同跌），拿两个等于半仓一个 bet；跨资产（如 A 股动量 + 黄金/海外/大宗）低相关第二腿才真正降回撤。自适应权重在第二腿弱时自动降仓，避免拖累收益。

## 回测方法

```bash
cd backend
# 对齐窗口
.venv/bin/python scripts/run_jq_rqalpha.py --start 2026-04-01 --end 2026-07-16 \
  --strategy tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.py --out data/quant_sim/dual_adapt_al
# 全窗口
.venv/bin/python scripts/run_jq_rqalpha.py --start 2026-04-01 --end 2026-08-11 \
  --strategy tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.py --out data/quant_sim/dual_adapt_full
# 模拟盘窗口
.venv/bin/python scripts/run_jq_rqalpha.py --start 2026-07-10 --end 2026-08-11 \
  --strategy tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.py --out data/quant_sim/dual_adapt_sim
```

指标用 `scripts/run_jq_rqalpha.py` 输出的 metrics（total_return/sharpe/max_drawdown/win_rate）。

## 关键前置修复（本分支附带，2 个引擎 bug）

探索中发现回测引擎两个严重问题，已修复并提交：

1. **哈希态不确定性**（`jqcompat.py` `_DayBarStore.preload`）：候选帧选择被 NaT 日期毒化，指数日线是否可用取决于候选顺序 → 同一回测换进程结果跳变（v5.4 在 +20.5%↔+22.5% 间）。
2. **窗口末端依赖**（分钟拆股复权锚定到窗口内最新事件）：588110 07-20 4:1 拆股使 as-of 早于拆股日拿到复权低价，与日线口径不一致 → 对齐/全窗口结果在 07-16 差 9pp。

修复后：aligned（ends 07-16）与 full（ends 08-11）在 07-16 净值完全一致，seed 0/1/5/7 结果一致。
