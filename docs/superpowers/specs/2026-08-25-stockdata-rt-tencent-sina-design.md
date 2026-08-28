# stockdata 实时价格源优化：腾讯/新浪快照 + 分钟bar合成

日期：2026-08-25
状态：已批准（2026-08-25，Q1/Q2/Q3 方案 A 均用户确认）

## 背景与问题

模拟盘实时取数链路：`live_feed.refresh` → `current_snapshot` → stockdata 服务
`get_realtime_snapshot`（sources.py ~515）。未覆盖/陈旧标的经 `NetworkPuller.fetch_many`
逐只走 `MootdxSource.get_minute_recent`（TDX TCP 拉当日分钟页）。两处延迟来源：

1. **TDX 源本身滞后**：通达信服务器 1m bar 推送延迟可达数十秒到分钟级（服务器间差异大）。
2. **3 分钟陈旧门槛**（sources.py ~547 硬编码）：内存最新 bar 距今不足 3 分钟不回源——
   为昂贵的逐只 TCP 拉取设计的节流，换廉价 HTTP 源后应大幅收紧。

参考 a-stock-data 项目（github.com/simonlin1212/a-stock-data）行情层：腾讯
`qt.gtimg.cn/q=` 批量 HTTP 快照（GBK，~88 字段，含现价/OHLC/昨收/累计量额/行情时间戳，
股票+ETF+指数全覆盖，实测不封 IP）。新浪实时行情该项目未收录；本设计采用经典
`hq.sinajs.cn/list=`（需 Referer 头，延迟最低 ~0.5-2s，同样批量）。

**用户决策记录：**
- Q1 数据源策略 = **A**：自动降级链 腾讯(主) → 新浪(备) → mootdx(兜底)，env 可强制单源。
- Q2 快照→分钟bar合成 = **A**：接受合成 bar（盘中近似），15:35 mootdx 全量落盘仍为权威数据。
- Q3 集成方式 = **A**：批量拉取层改造 `fetch_many`，一次 HTTP 拿全部 todo 标的。

## 方案

### 新增模块 `backend/app/services/stockdata/rt_sources.py`

1. **`TencentRTSource` / `SinaRTSource`**：批量快照拉取，统一归一化输出
   `{code: RTQuote(price, prev_close, open, high, low, cum_volume, cum_amount, quote_time)}`。
   - 腾讯：`https://qt.gtimg.cn/q=sh600519,sz000001,...`，一次请求全部标的；
     字段索引按 a-stock-data 实测校准表（3=现价 4=昨收 5=今开 6=累计量 33=最高 34=最低
     37=累计额 30=时间戳）；GBK 解码；requests Session Keep-Alive + 正常 UA。
   - 新浪：`https://hq.sinajs.cn/list=sh600519,...` + `Referer: https://finance.sina.com.cn`；
     逗号序字段（今开,昨收,现价,最高,最低,…,累计量(股),累计额(元),日期,时间）；GBK。
   - 前缀映射复用平台符号口径：`.SH`→sh、`.SZ`→sz（平台无北交所代码，与现状一致不覆盖）。

2. **`BarSynthesizer`**：连续快照 → `_MINUTE_COLS` 分钟 bar 帧（per-symbol 状态：
   `{交易日, 最新价, 上次累计量/额, 当前分钟bar}`）：
   - 同一分钟内多次快照 → 更新该 bar H/L/C（close=最新价）与量额差分；
   - 跨分钟 → 开新 bar（open 取上一拍价格）；换日重置状态；
   - 差分负值（源重置/乱序）→ clamp 0 并以本次累计值重建基线；
   - 首拍无基线 → 只建价格 bar、量额记 0（保守不虚增；自举后差分自然准确）；
   - bar datetime = 快照时间戳向下取整分钟；缺时间戳 → 本地时间取整。

### `sources.py` 改造（协议/客户端/前端零变更）

`NetworkPuller.fetch_many(todo)` 改为编排链：

```
① per-symbol 结果缓存（TTL STOCKDATA_RT_RESULT_TTL，默认 3s）命中直接复用
② 冷启动自举：交易时段内、既无内存 bar 也无当日分区行的标的
   → 现有 mootdx 路径拉一次「今日迄今」分钟（每标的每天仅一次）
③ 腾讯批量（剩余标的，1 次 HTTP）→ BarSynthesizer 合成
④ 失败/解析缺失集 → 新浪批量（1 次 HTTP）
⑤ 仍失败 → 逐只 mootdx 兜底（现有线程池 + 30s 墙钟守护不动）
帧并入 minute_store（现有逻辑不变）
```

- **冷启动自举保留盘中启动的全天上下文**：现在模拟盘盘中启动可从 mootdx 拿全天迄今
  分钟历史，纯快照流只能从启动时刻开始——自举保证策略 `history_bars(1m)` 行为不回退。
- **陈旧门槛收紧并参数化**：`get_realtime_snapshot` 的硬编码 3min 改为
  `STOCKDATA_RT_STALE_SEC`（默认 10s）；判定用 `max(bar_dt, 最近快照时间)`——
  无成交冷门股不会因「没有新 bar」被反复判陈旧。

### env 配置

| 变量 | 默认 | 说明 |
|------|------|------|
| `STOCKDATA_RT_SOURCE` | `auto` | `auto\|tencent\|sina\|mootdx`；设 mootdx 即一键回滚旧行为 |
| `STOCKDATA_RT_STALE_SEC` | `10` | 陈旧门槛（原硬编码 180s） |
| `STOCKDATA_RT_RESULT_TTL` | `3.0` | per-symbol 结果缓存 TTL |
| `STOCKDATA_RT_HTTP_TIMEOUT` | `3.0` | 批量 HTTP 超时 |

### 错误处理

- 整批失败（超时/网络/解析异常）→ 下一级源；单只字段缺失/停牌（价格=0 不造 bar）→ 进失败集下传；
- 全部源失败 → 沿用内存旧帧（现有行为）；非交易时段不触网（不变）；
- 单位一致性：实现第一步用几只股票+ETF 实测腾讯累计量 vs mootdx 同时刻量确定倍率
  （手/股），合成器输出归一到现有 mootdx 分钟路径单位，保证内存库与落盘分区口径一致。

## 不做的事（YAGNI）

- 不加主动后台轮询（仍保持客户端请求驱动 lazy 回源）；
- 不动历史回源路径（15:35 mootdx 全量同步仍是权威落盘）；
- 不动东财（封 IP 风险，无必要）；不做 websocket/流式推送；
- 北交所覆盖不做（平台代码只有 .SH/.SZ，mootdx 本就跳过）；
- 协议、客户端、前端零变更。

## 测试方案

新增 `backend/tests/quant/test_stockdata_rt_sources.py`（mock HTTP，录制真实响应 fixture）：

1. 前缀映射 + GBK 解析（腾讯/新浪 fixture：股票/ETF/停牌/异常行）；
2. 合成器：同分钟多 tick、跨分钟开新 bar、换日重置、负差分防护、首拍零量；
3. 降级链：腾讯挂→新浪接→mootdx 兜底；env 强制单源生效；
4. 结果 TTL 去重、陈旧门（含快照时间修正）、自举每标的每日一次；
5. 更新 `test_stockdata_sources.py` 中依赖硬编码 3min 陈旧门的既有用例。

验收：交易时段起服务，对若干股票/ETF curl `current_snapshot` 对比新旧路径现价延迟；
合成 close 与腾讯现价一致；`uv run --extra dev pytest` 全绿 + ruff/mypy 通过。

## 回滚

`STOCKDATA_RT_SOURCE=mootdx` 恢复旧行为；无协议/存储/迁移变更。
