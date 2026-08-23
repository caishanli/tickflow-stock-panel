# 模拟盘详情页分时图修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修掉模拟盘详情页分时弹窗「一条直线」——Y轴按实际波动自适应、显示该标的当日全部买卖标记、ptrade `.SS` 代码出图。

**Architecture:** 前端 `EChartsIntraday` 剔除均价线参与 Y 轴范围计算 + volume 单位自探测修均价线；`QuantSim` 传该标的全部成交标记、持仓行开在入场当日；后端三处镜像的后缀映射函数加 `"SS"` 沪市后缀。无 API 契约变化。

**Tech Stack:** React 18 + TS + ECharts（前端）；FastAPI + pytest（后端）。

**Spec:** `docs/superpowers/specs/2026-08-22-sim-detail-minute-kline-fix-design.md`

## Global Constraints

- 后端命令一律 `cd backend && uv run --extra dev <cmd>`（dev 依赖不在基础 venv）
- ruff line-length 100；`uv run --extra dev ruff check app`、`uv run --extra dev mypy app` 必须过
- 前端无测试脚本：验证 = `cd frontend && pnpm lint && pnpm build`
- 不改 `data/` 下任何文件；不加注释以外的多余改动；**不加代码注释除非必要**（本计划代码块中的注释为保留/必要的中文业务注释，遵循现有文件风格）
- 分支 `fix/sim-detail-minute-kline-flat`，每个 Task 一次 commit

---

### Task 1: 后端 `.SS` 后缀归一化（三处 + 单测）

**Files:**
- Modify: `backend/app/api/kline.py:34-39`（`_to_jq_code`）
- Modify: `backend/app/services/stockdata/sources.py:40-44`（`_to_jq`）、`:47-58`（`_is_index`）
- Modify: `backend/app/quant/datasource/network_client.py:36-40`（`_to_jq`）
- Test: `backend/tests/test_kline_stockdata_source.py`、`backend/tests/quant/test_stockdata_sources.py`、`backend/tests/quant/test_network_client.py`

**Interfaces:**
- Consumes: 无（独立改动）
- Produces: `_to_jq_code("518880.SS") == "518880.XSHG"`（三处同语义）；`_is_index("000300.SS") is True`

- [ ] **Step 1: 写失败测试**

`backend/tests/test_kline_stockdata_source.py` 的 `test_to_jq_code` 追加两行：

```python
def test_to_jq_code():
    assert _to_jq_code("000001.SZ") == "000001.XSHE"
    assert _to_jq_code("600000.SH") == "600000.XSHG"
    assert _to_jq_code("600000.XSHG") == "600000.XSHG"
    assert _to_jq_code("920001.BJ") == "920001.XSHE"  # 未知后缀按深市
    assert _to_jq_code("518880.SS") == "518880.XSHG"  # ptrade 沪市后缀
    assert _to_jq_code("513360.SS") == "513360.XSHG"  # ptrade 沪市后缀
```

`backend/tests/quant/test_stockdata_sources.py`：import 块加 `_to_jq`（按字母序插在 `_is_index` 后）：

```python
from app.services.stockdata.sources import (
    DataSources,
    MinuteMemoryStore,
    NetworkPuller,
    _is_index,
    _pull_recent_guarded,
    _to_jq,
)
```

`test_is_index_suffix_based` 追加一行，并在其后新增一个测试：

```python
def test_is_index_suffix_based():
    assert _is_index("000001.XSHE") is False   # 深市 000001 平安银行是股票
    assert _is_index("000157.XSHE") is False   # 深市 000157 中联重科是股票
    assert _is_index("000300.XSHG") is True    # 沪市 000xxx 是指数
    assert _is_index("000300.SS") is True      # ptrade 沪市后缀
    assert _is_index("399006.XSHE") is True    # 399 深证指数，任意市场
    assert _is_index("512670.XSHG") is False   # ETF 不是指数


def test_to_jq_accepts_ptrade_ss_suffix():
    assert _to_jq("518880.SS") == "518880.XSHG"
    assert _to_jq("600000.SH") == "600000.XSHG"
    assert _to_jq("000001.SZ") == "000001.XSHE"
```

`backend/tests/quant/test_network_client.py` 末尾追加：

```python
def test_client_to_jq_accepts_ptrade_ss():
    from app.quant.datasource.network_client import _to_jq

    assert _to_jq("518880.SS") == "518880.XSHG"
    assert _to_jq("000001.SZ") == "000001.XSHE"
```

- [ ] **Step 2: 跑测试确认失败**

```bash
cd backend && uv run --extra dev pytest tests/test_kline_stockdata_source.py::test_to_jq_code tests/quant/test_stockdata_sources.py::test_is_index_suffix_based tests/quant/test_stockdata_sources.py::test_to_jq_accepts_ptrade_ss_suffix tests/quant/test_network_client.py::test_client_to_jq_accepts_ptrade_ss -q
```

预期：4 个测试 FAIL（`.SS` 断言得到 `.XSHE`/False）。import 失败（`_to_jq` 不在 import 列表时）也算失败证据。

- [ ] **Step 3: 实现（三处后缀集合加 `"SS"`）**

`backend/app/api/kline.py`：

```python
def _to_jq_code(symbol: str) -> str:
    """000001.SZ → 000001.XSHE; 600000.SH → 600000.XSHG; 未知后缀按深市。"""
    pure, _, suf = symbol.rpartition(".")
    if not pure:
        return symbol
    return pure + (".XSHG" if suf in ("SH", "SS", "XSHG") else ".XSHE")
```

`backend/app/services/stockdata/sources.py` `_to_jq`：

```python
def _to_jq(code: str) -> str:
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".XSHG" if suf in ("SH", "SS", "XSHG") else ".XSHE")
```

`backend/app/services/stockdata/sources.py` `_is_index`（docstring 同步更新）：

```python
def _is_index(code: str) -> bool:
    """指数判定：399 开头任意市场；000xxx 仅沪市（SH/SS/XSHG）是指数。

    深市 000xxx（如 000001 平安银行）是股票，不能误走指数通道（mootdx 深市
    000xxx 走 index_bars 返回空）。同 mootdx_src._is_index。
    """
    pure = code.split(".", 1)[0]
    suffix = code.split(".", 1)[1] if "." in code else ""
    if pure.startswith("399"):
        return True
    return (suffix in ("SH", "SS", "XSHG") and pure.startswith("000")
            and len(pure) == 6 and not pure.startswith("0000"))
```

`backend/app/quant/datasource/network_client.py` `_to_jq`：

```python
def _to_jq(code: str) -> str:
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".XSHG" if suf in ("SH", "SS", "XSHG") else ".XSHE")
```

- [ ] **Step 4: 跑测试确认通过**

```bash
cd backend && uv run --extra dev pytest tests/test_kline_stockdata_source.py tests/quant/test_stockdata_sources.py tests/quant/test_network_client.py -q
```

预期：全部 PASS。

- [ ] **Step 5: lint + 类型检查**

```bash
cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app
```

预期：无错误。

- [ ] **Step 6: Commit**

```bash
git add backend/app/api/kline.py backend/app/services/stockdata/sources.py backend/app/quant/datasource/network_client.py backend/tests/test_kline_stockdata_source.py backend/tests/quant/test_stockdata_sources.py backend/tests/quant/test_network_client.py
git commit -m "fix(kline): .SS ptrade 沪市后缀归一化——修模拟盘弹窗分时/日K查无数据空白"
```

---

### Task 2: EChartsIntraday Y轴自适应 + 均价线单位自探测

**Files:**
- Modify: `frontend/src/components/EChartsIntraday.tsx`（`computeAvgPrice` L49-60、`buildOption` L151-254 及调用点 L543）

**Interfaces:**
- Consumes: `MinuteKlineRow`（`@/lib/api`，字段 volume/amount/close 不变）
- Produces: `computeAvgPrice`、`buildOption` 内部行为变化；`buildOption` 签名**不变**（`showAvgLine` 参数仍被函数体内均价 series 条件展开 L430 使用）；组件对外 Props 不变

- [ ] **Step 1: `computeAvgPrice` 单位自探测**

替换 L49-60 整个函数为两个函数：

```ts
/** 探测分钟数据 volume 单位: 股(×1) / 手(×100)。
 *  stockdata 与本地 mootdx parquet 为股; TickFlow SDK vol 疑似手。
 *  依据 amount ≈ volume(股)×price: median(amount/volume) ≈ 100×median(close) 判为手。 */
function detectVolumeMultiplier(data: MinuteKlineRow[]): number {
  const ratios: number[] = []
  const closes: number[] = []
  for (const d of data) {
    if (!(d.volume > 0) || !(d.close > 0)) continue
    ratios.push(d.amount / d.volume)
    closes.push(d.close)
    if (ratios.length >= 60) break
  }
  if (ratios.length === 0) return 1
  const med = (arr: number[]) => {
    const s = [...arr].sort((a, b) => a - b)
    return s[Math.floor(s.length / 2)]
  }
  const ratio = med(ratios) / med(closes)
  return ratio >= 30 && ratio <= 300 ? 100 : 1
}

function computeAvgPrice(data: MinuteKlineRow[]): number[] {
  // 分时均线 = 累计成交额 / 累计成交量(单位自适应: 股×1 / 手×100)
  const mult = detectVolumeMultiplier(data)
  const result: number[] = []
  let sumAmt = 0
  let sumVol = 0
  for (const d of data) {
    sumAmt += d.amount
    sumVol += d.volume * mult
    result.push(sumVol > 0 ? sumAmt / sumVol : d.close)
  }
  return result
}
```

- [ ] **Step 2: `buildOption` maxDiff 剔除均价 + 自适应统一边距（签名不动）**

`buildOption` 签名保持原样——`showAvgLine` 参数仍被函数体内均价 series 条件展开使用，不可删除。

maxDiff 计算（原 L204-212）——`priceArrays` 不再包含 `avgData`：

```ts
  let yMin: number | undefined
  let yMax: number | undefined
  let maxDiff = 0
  if (isValidPrice(prevClose) && data.length > 0) {
    // 均价线恒在 [minLow, maxHigh] 内, 不参与范围计算(免疫均价单位错误, 范围贴合实际波动)
    const priceArrays = [closes, highs, lows]
    for (const arr of priceArrays) {
      for (const v of arr) {
        if (!isValidPrice(v)) continue
        const diff = Math.abs(v - prevClose)
        if (diff > maxDiff) maxDiff = diff
      }
    }
```

自适应分支（原 L238-253）替换为：

```ts
    } else {
      // 自适应模式: Y 轴贴合实际波动(留 10% 边距), 昨收居中; 涨跌停带仅作上限钳制
      if (showLimitLines) {
        const { limitUp, limitDown } = getLimitPrices(prevClose, priceLimit)
        const limitDiff = Math.max(limitUp - prevClose, prevClose - limitDown)
        maxDiff = Math.min(maxDiff * 1.1, limitDiff)
      } else if (maxDiff > 0) {
        maxDiff *= 1.1
      }
      // 至少保证一个可视范围 (防止数据平时 maxDiff=0)。指数不使用涨跌停范围，最小范围要更紧，否则低波动指数会被压成横线。
      const minDiff = showLimitLines ? prevClose * 0.01 : prevClose * 0.001
      if (maxDiff < minDiff) maxDiff = minDiff
      yMin = prevClose - maxDiff
      yMax = prevClose + maxDiff
    }
```

调用点 L543 不变（`showAvgLine` 实参保留）。

- [ ] **Step 3: lint + 构建验证**

```bash
cd frontend && pnpm lint && pnpm build
```

预期：无错误。

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/EChartsIntraday.tsx
git commit -m "fix(chart): 分时Y轴贴合实际波动——maxDiff剔除均价线+自适应统一10%边距+volume单位自探测修均价线"
```

---

### Task 3: QuantSim 标记全量化 + 持仓行开在入场当日

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx`（helper 加在 `toMarkerAction` 后 ~L102；持仓 onClick L874-884；成交 onClick L956-965）

**Interfaces:**
- Consumes: `sortedTrades: any[]`（SimDetail 内 L711 已有）、`parseTradeTime`、`toMarkerAction`、`IntradayMarker`（均已 import/定义）
- Produces: `buildSymbolMarkers(trades: any[], sym: string): IntradayMarker[]`

- [ ] **Step 1: 新增 helper（`toMarkerAction` 函数之后）**

```ts
/** 该标的全部成交 → 分时标记 (弹窗按选中日期过滤渲染) */
function buildSymbolMarkers(trades: any[], sym: string): IntradayMarker[] {
  if (!sym) return []
  const out: IntradayMarker[] = []
  for (const t of trades) {
    if ((t.code ?? '') !== sym || typeof t.price !== 'number') continue
    const parsed = parseTradeTime(t.ts)
    if (!parsed) continue
    out.push({ date: parsed.date, time: parsed.time, price: t.price, action: toMarkerAction(t.action) })
  }
  return out
}
```

- [ ] **Step 2: 持仓行 onClick（L874-884）**

```tsx
                      <tr key={sym}
                        onClick={() => {
                          setPreview({
                            symbol: sym,
                            name: p.name ?? '',
                            date: parseTradeTime(p.entry_ts)?.date,
                            markers: buildSymbolMarkers(sortedTrades, sym),
                          })
                        }}
```

- [ ] **Step 3: 成交记录行 onClick（L956-965）**

```tsx
                    <tr key={i}
                      onClick={() => {
                        setPreview({
                          symbol: t.code ?? '',
                          name: t.name ?? '',
                          date: String(t.ts ?? '').slice(0, 10),
                          markers: buildSymbolMarkers(sortedTrades, t.code ?? ''),
                        })
                      }}
```

- [ ] **Step 4: lint + 构建验证**

```bash
cd frontend && pnpm lint && pnpm build
```

预期：无错误（`preview` state 类型 `date?: string` 兼容 `string | undefined`）。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/quant/pages/QuantSim.tsx
git commit -m "fix(quant): 模拟盘分时弹窗显示该标的当日全部买卖标记, 持仓行开在入场当日"
```

---

### Task 4: 全量验证 + 手动验收

**Files:** 无新改动（验证任务）

- [ ] **Step 1: 后端全量相关测试**

```bash
cd backend && uv run --extra dev pytest tests/test_kline_stockdata_source.py tests/quant/test_stockdata_sources.py tests/quant/test_network_client.py tests/quant/test_ptradecompat.py -q
```

预期：全部 PASS。

- [ ] **Step 2: 后端 lint + mypy**

```bash
cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app
```

- [ ] **Step 3: 前端 lint + 构建**

```bash
cd frontend && pnpm lint && pnpm build
```

- [ ] **Step 4: 手动验收（dev.sh 起服务 → 模拟盘详情页）**

后台拉起（勿裸 nohup）：

```bash
setsid ./dev.sh > /tmp/tickflow-dev.log 2>&1 </dev/null & disown
```

另起命令验证端口后逐项检查：

1. 点**成交记录行**：波动占图高比例合理（不再贴成直线），黄色均价线在价格带内而非贴底，当日全部 B/S/止损标记可见
2. 点**持仓行**：直接打开入场当日，B 点落在买入分钟上
3. 弹窗内切换日期：标记跟随日期显隐
4. ptrade 账户（`.SS` 代码，如五福v5.4-ptrade对齐）：分时/日K 出图不再空白
5. 回归：自选列表迷你分时、指数页分时、个股页分时无样式回归

预期：1-4 全部满足；5 无回归。

- [ ] **Step 5: 验收通过后收尾**

```bash
git log --oneline custom-main..HEAD
```

确认 3 个 feature commit + 1 个 spec commit。不 push、不合并（等用户指示）。
