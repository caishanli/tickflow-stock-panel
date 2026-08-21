# stockdata 日线日期文件 LRU 缓存设计

日期：2026-08-21
状态：已批准（2026-08-21，四节逐节确认）

## 背景与问题

stockdata 服务（`backend/app/services/stockdata/`）RSS 实测 ~1.16GB，匿名堆占 ~1.12GB。根因：

- `preload_daily(400)`（sources.py `_load_daily`）把全市场股票+ETF 400 天日线（~268 万行）读成一个 Polars 整帧，经 `DedupCache` 60s 驻留；Python 分配器 GC 后不归还 OS，RSS 稳定在高位。
- `get_daily` 每请求按 chunk 扫日期分区 + symbol 过滤，另存 `daily:` 请求级 60s 缓存。
- 设计规格（`2026-08-05-stockdata-service-design.md` 第 82/129/138 行）要求 `preload_daily` 全市场整帧批量返回，但未量化该帧的内存预算——这是设计缺口。

规格本意（第 112-123 行）：只有当日分钟内存库驻留（lazy、几十 MB），其余数据读盘按需。

## 方案（已批准：方案 1）

新增「日线日期文件缓存」`DayFileCache`：日线分区按日存储、每文件含全市场标的，读取时整文件载入内存（同文件其他标的后续请求直接命中），按最后访问时间超时卸载 + 容量上限淘汰。绝不缓存 400 天全市场整帧。

被否方案：请求级缓存+LRU 双层（语义重叠、双份内存）；客户端侧 LRU（每模拟盘进程各持一份，内存翻倍）。

## 第 1 节：缓存语义

**键**：`(subdir, date)`，`subdir ∈ {kline_daily, kline_etf_daily, kline_index_daily}`，`date = YYYY-MM-DD`。

**值**：该日分区文件的全市场整帧，固定 8 列（symbol/date/open/high/low/close/volume/amount），**原始单位存盘**（股票 volume 为「手」，不预换算）。换算/投影统一在读取侧做，缓存不存变体。

**访问语义**：
- `get(subdir, date)`：命中返回帧并刷新该文件最后访问时间；未命中返回 None（不加载）。
- `load(subdir, date)`：日期文件级 single-flight 读盘（同键并发只读一次），写入缓存 + 刷新访问时间。
- 读取侧（get_daily/preload_daily）按需从缓存过滤 symbols 后拼接返回新帧，不动缓存原帧。

**卸载规则**（纯后台清扫，访问时不清理）：
- 清扫线程每 10s 跑一次：
  1. 先卸载 `最后访问时间 > 60s` 的文件；
  2. 若仍超容量上限（60 个文件），按最后访问时间从旧到新踢，直到 ≤60。

**并发**：dict + `threading.Lock`（读写都加锁；帧引用替换原子，读侧拿到的旧帧照常可用）。清扫与载入不互斥：清扫只删键，不碰正在被读取的帧。

**启动/换日**：无预载无预分配；不跨日清理（日期文件天然过期即卸载，无需 00:00 钩子）。

## 第 2 节：服务端实现

1. `DayFileCache` 类放 `sources.py`（与 `MinuteMemoryStore` 同文件、同风格），scheduler 持有实例引用。
2. **`get_daily(codes, start, end)` 重写**：
   - 日期规范化逻辑保留（%Y%m%d → ISO，兼容 jqcompat 与 rqalpha_bridge 两种格式）。
   - 遍历 `[start, end]` 内每个日期 × 3 个 subdir：`get()` 未命中则 `load()`（single-flight 读盘）。
   - 收集各文件帧 → 过滤请求 symbols → 投影 8 列 → 股票 `volume×100` → `_normalize_etf_volume_unit` → concat 返回。
   - **删除原 `daily:` 请求级 60s 缓存**（LRU 已覆盖；重复 chunk 请求直接命中内存帧）。
   - 空结果返回空帧，语义与现状一致。
3. **`preload_daily(lookback, asof)` 重写**：
   - 维持现语义：只含股票 + ETF（**不含指数**，避免污染下游 ETF 宇宙），按 asof 截断。
   - 实现：逐日文件经 LRU 取/载 → concat → 返回；帧不驻留（LRU 按 60s/60 文件自然淘汰）。
   - 离线回测一次性调用，拼帧成本秒级，可接受。
4. **清扫线程**：scheduler 启动时开后台线程（与现有 backfill/cron 线程并列），每 10s 调 `cache.sweep()`；debug 级日志。
5. **单元测试**（`test_stockdata_sources.py` 或新建 test 文件）：命中/未命中/并发 single-flight/60s 过期卸载/容量踢旧/`get_daily` 与旧实现结果逐行一致/`preload_daily` 语义不变（不含指数、asof 截断、volume 口径）。

## 第 3 节：客户端改动

1. **模拟盘（在线）**：删除 `runner.py` 两处 `dm.preload_daily()`（`_make_dm` 启动处 + `_pre_market` 盘前处）。依据：在线模式日线按需读，LRU 命中后逐块 chunk 请求秒级；按需读每次都是新数据，manager.py:440-443 记录的"盘前不预载 → 全市场成交额只刷到部分标的"陈旧回归（旧实现预载一次后 `_daily_mem` 停留）不再适用。注释写明原因，防止后人"修复"回去。
2. **离线回测**：`rqalpha_bridge.py` 四处 `dm.preload_daily()` 不动——offline 客户端仍需整帧预热进 `_daily_mem`（服务端经 LRU 拼帧返回，行为不变，只省服务端驻留）。
3. **`network_client.py` / `DataManager` / `jqcompat`**：零改动——协议、接口、返回帧结构全不变，LRU 是服务端纯内部实现。
4. **客户端 `_daily_mem` 会话驻留**：本设计不动（回测/模拟盘进程内按标的缓存日线，随进程退出释放，不占 stockdata RSS）。后续如需压模拟盘进程内存，另行设计。

## 第 4 节：验证方案

1. 单元测试（第 2 节列）：DayFileCache 命中/未命中/并发 single-flight/60s 过期卸载/容量踢旧；`get_daily` 新旧实现结果逐行一致（固定日期区间 + symbols，对比重构前输出）；`preload_daily` 语义不变。
2. 全量回归：`pytest tests/quant/`（除既有 flaky `test_h4_h5_universe_writable_and_run_daily_fires_in_daily_mode`）——70978ed5 / dual_v54 / wufu_v52 对齐测试（jq vs ptrade 回测，走新 get_daily/preload_daily 路径）+ 模拟盘测试。
3. 模拟盘实测对齐：账户 `960366ab`（五福v5.4-ptrade对齐）重启跑一遍，对比重启前后 sim_trades/净值快照逐日零差——证明在线按需读路径正确。
4. 内存实测：重启 stockdata 服务（改代码不热重载，需手动重启，guardian 自动拉起）→ 记录 RSS 基线 → 跑一次 70978ed5 对齐回测（触发全路径）→ 观察 RSS 峰值与回落。预期 RSS 从 ~1.16GB 降至数百 MB（运行时 + 分钟库 + LRU ≤30MB）。
5. 验收标准：对齐测试零差 + 模拟盘重启前后零差 + stockdata RSS 峰值显著下降（目标 < 600MB）。

## 不做的事（YAGNI）

- 分钟分区的日文件 LRU（分钟已有当日内存库 + 分区读，另行设计）。
- 客户端 `_daily_mem` 改造（见第 3 节）。
- 容量上限参数化（60 固定；如需可后续加 env）。
- 访问时懒卸载（用户明确选纯后台清扫）。