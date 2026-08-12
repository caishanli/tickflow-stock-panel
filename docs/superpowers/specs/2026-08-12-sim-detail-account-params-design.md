# 模拟盘详情页展示账户参数（初始资金/止损线）— 设计

日期：2026-08-12，分支：fix/sim-reset-clear-tabs（继续沿用）

## 目标

量化模拟盘详情页顶栏目前只显示「策略 · 频率 · 开始日期」，新建账户时的两个参数——初始资金、止损线——无处可见。在顶栏信息行补齐。

## 范围

- 仅 `frontend/src/quant/pages/QuantSim.tsx`（SimDetail 组件），无后端改动。
- 数据来自现有 `st.account`（`capital`/`stop_loss` 字段后端 status 接口已返回）。

## 交互

- 顶栏信息行扩展为：`初始资金 100000.00 · 止损 5.00% · 策略 wufu v5.2 · 分钟级 · 自 2026-07-10`。
- 资金格式化复用现有 `fmtNum`（与列表页口径一致）。
- 止损新增 `fmtStopLoss` 小工具：`(v * 100).toFixed(2) + '%'`，非数值显示 `—`；不使用 `fmtPct`（其输出带 `+` 号，对止损参数不合适）。

## 实现要点

- 顶栏 `<span className="text-xs text-muted">` 内，在「策略」前插入资金与止损两项。
- `fmtStopLoss` 放文件顶部 `fmtNum`/`fmtPct` 旁。
