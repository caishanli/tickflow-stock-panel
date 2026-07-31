# ETF 数据隔离 + DuckDB 接入 + 启动回源

## 背景

DuckDB 迁移后，ETF 日线/分钟数据混在 stock 表里（`kline_daily`/`kline_minute`），`kline_etf_daily`/`kline_etf_minute` 为空。jqengine 回测 DataManager 仍读 parquet 缓存（已删除），导致 wufu v5.2 回测 0 笔交易。

## 目标

1. ETF 日线/分钟数据与 stock 完全隔离，分别存入独立表
2. jqengine DataManager 直接从 DuckDB 读取，不依赖 parquet 缓存
3. 主进程启动时自动检查缺失 ETF 数据并回源补齐
4. 实时交易时段 ETF 分钟数据写入正确表

## 一、DuckDB 数据迁移（一次性脚本）

写 `scripts/migrate_etf_to_separate_table.py`，基于 `instruments_etf` 表的实际 ETF 代码迁移：

```sql
-- ETF 日线
INSERT OR REPLACE INTO kline_etf_daily
SELECT k.* FROM kline_daily k
JOIN instruments_etf e ON k.symbol = e.symbol;
DELETE FROM kline_daily WHERE symbol IN (SELECT symbol FROM instruments_etf);

-- ETF enriched（如有）
INSERT OR REPLACE INTO kline_etf_enriched
SELECT k.* FROM kline_daily_enriched k
JOIN instruments_etf e ON k.symbol = e.symbol;
DELETE FROM kline_daily_enriched WHERE symbol IN (SELECT symbol FROM instruments_etf);

-- ETF 分钟线
INSERT OR REPLACE INTO kline_etf_minute
SELECT k.* FROM kline_minute k
JOIN instruments_etf e ON k.symbol = e.symbol;
DELETE FROM kline_minute WHERE symbol IN (SELECT symbol FROM instruments_etf);
```

迁移前后打印行数对比，确保数据不丢失。

## 二、修复分钟线写入路由

### 2a. MinuteKService（实时 mootdx）

`app/services/minute_k_service.py`：

- `_write_to_duckdb(df)` → `_write_to_duckdb(df, asset_type="stock")`
- 根据 `asset_type` 选择表：`"kline_etf_minute" if asset_type == "etf" else "kline_minute"`
- `_fetch_etf_minute_k()` 调用时传 `asset_type="etf"`
- `_fetch_stock_minute_k()` 调用时传 `asset_type="stock"`

### 2b. sync_and_persist_minute（批量同步）

`app/services/kline_sync.py`：

- `sync_and_persist_minute()` 不再硬编码 `asset_type="stock"`
- 根据待写入的 symbol 判断：查 `instruments_etf` 判断是否 ETF
- `_write_minute_partition()` 已有正确路由逻辑，只需传入正确 asset_type

## 三、jqengine DataManager 接入 DuckDB

### 3a. DuckDBSource 类

`app/quant/jqengine/datasource/manager.py` 新增：

```python
class DuckDBSource:
    name = "duckdb"

    def get_daily(self, code, start, end):
        # JQ 代码转 DuckDB：.XSHG → .SH, .XSHE → .SZ
        # 根据 instruments_etf 判断路由 kline_daily 或 kline_etf_daily

    def get_minute(self, code, date):
        # 根据 instruments_etf 判断路由 kline_minute 或 kline_etf_minute

    def get_etf_list(self):
        # SELECT symbol FROM instruments_etf

    def get_stock_list(self):
        # SELECT symbol FROM instruments
```

### 3b. SOURCES 与优先级

```python
SOURCES = {"duckdb": DuckDBSource, "mootdx": MootdxSource, "astock": AStockSource}
```

`config.py` 默认优先级：`"duckdb,mootdx,astock"`

### 3c. preload_daily 改造

从 DuckDB 批量加载全市场日线：

```python
def preload_daily(self):
    # 优先从 DuckDB 批量加载
    if "duckdb" in self.sources:
        self._preload_from_duckdb()
    # 保留 mootdx/astock 作为 fallback
    ...
```

`_preload_from_duckdb()`：一次查询 `kline_daily` + `kline_etf_daily`，按 symbol 分帧存入 `_daily_mem`。

## 四、主进程启动回源

`app/main.py` lifespan 中，MinuteKService 启动前插入 ETF 数据检查：

```python
# ETF 数据完整性检查
await _backfill_etf_data(repo)
```

`_backfill_etf_data()` 逻辑：
1. 查 `instruments_etf` 获取全量 ETF 列表
2. 查 `kline_etf_daily` 每个 ETF 的最新日期
3. 查 `kline_etf_minute` 每个 ETF 的最新日期
4. 对缺失/过期的 ETF，从 TickFlow API / mootdx 回源补齐日线和分钟数据
5. 完成后 log 输出回源统计
6. 进入实时模式（MinuteKService 正常启动）

## 五、清理残留 parquet

删除 `data/quant_kline/daily/` 和 `data/quant_kline/minute/` 下所有 `.parquet` 文件。

## 涉及文件

| 文件 | 改动 |
|------|------|
| `scripts/migrate_etf_to_separate_table.py` | 新增：一次性迁移脚本 |
| `app/services/minute_k_service.py` | 修复：ETF 分钟写入 kline_etf_minute |
| `app/services/kline_sync.py` | 修复：批量分钟同步 asset_type 路由 |
| `app/main.py` | 新增：启动时 ETF 数据回源检查 |
| `app/quant/jqengine/datasource/manager.py` | 新增：DuckDBSource + preload 改造 |
| `app/quant/jqengine/config.py` | 修改：默认优先级 duckdb,mootdx,astock |
| `data/quant_kline/` | 删除：残留 parquet 缓存 |

## 验证

1. 迁移后 DuckDB 行数对比：`kline_daily` 减少 ETF 行数 = `kline_etf_daily` 增加行数
2. 主进程启动日志：ETF 回源统计
3. wufu v5.2 回测：交易笔数 > 0，与 JoinQuant 参考对比
4. 实时模式：ETF 分钟数据写入 `kline_etf_minute` 而非 `kline_minute`
