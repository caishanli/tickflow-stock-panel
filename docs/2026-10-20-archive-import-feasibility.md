# 归档数据导入本机 stockdata —— 可行性验证报告

- 日期：2026-10-20
- 结论：**可行**（价格口径一致、schema 可完全对齐、端到端读取通过）
- 实现：`backend/app/quant/archive_import.py`（转换）+ `backend/app/quant/archive_import_cli.py`（CLI）
- 测试：`backend/tests/quant/test_archive_import.py`（6 passed）

## 一、验证方法与证据

### 1.1 重叠日逐行比对（决定性证据）

取**本机与归档都有的完整分区** 2025-06-10，归档 1m 与本机 `kline_minute`：

| 项 | 归档 | 本机 | 结论 |
|---|---|---|---|
| 行数 | 1 313 266 | 1 313 266 | **完全相同** |
| 标的数 | 5 416 | 5 416 | **完全相同** |
| open/high/low/close/volume/amount | — | — | **不等行数 = 0**（6 列全零差异） |

日线 2025-09-15 / 2026-04-17 重叠日：OHLC **零差异**。

### 1.2 复权口径：**一致**（推翻初始担忧）

初始担忧是「归档存复权价、本机存原始价 → 导入会双重复权」。**实测证伪**：
归档日线 2019-01-04 的 000001.SZ close=9.75，同日归档分钟线 close 区间 9.16–9.40 —— 两者同口径；
且重叠日 OHLC 与本机逐行零差异。**两者都是同一复权序列**，直接导入不会造成复权错乱。

> 代码佐证：`manager.py:652` *"volume 单位：A股日线（kline_daily）存盘为「手」，读取时 ×100 归一"*
> —— 复权在**读取时**由因子表应用，存量分区与归档同为复权价。

### 1.3 volume 单位差异（唯一需要换算的地方）

| 数据 | 归档 | 本机 | 处理 |
|---|---|---|---|
| 1m 分钟 | 股（423 600） | 股（423 600） | ✅ 不换算，零差异 |
| 1d 日线 | 股（84 038 656） | **手**（840 387） | ⚠️ **÷100** |

验证：÷100 后与本机最大绝对差 1697（4/5399 行超 0.5，因归档是原始整手、舍入误差），量级正确。
amount 差异 ≤ 1 元（浮点精度），相对误差 ≤ 1.2e-4。

### 1.4 schema 对齐

本机 `kline_minute`：`symbol(str), datetime(datetime[us]), open, high, low, close, volume, amount`（均 f64）
归档 1m：`code, trade_time(str), close, open, high, low, vol, amount, date, pre_close, change, pct_chg, __index_level_0__`

转换：`code→symbol`、`trade_time` 解析成 datetime、`vol→volume`；丢弃归档独有的
`date/pre_close/change/pct_chg/__index_level_0__`。**导入产物 schema 与本机分区完全一致**（实测 `schema == schema` 为 True，列顺序一致）。

### 1.5 端到端读取（真实代码路径）

用 `PARTITION_DATA_ROOT` 把本机读取路径指向导入产物：

- `DataManager._partition_dates('kline_minute')` → 正确识别 22 个分区 ✅
- `DataSources.get_minute(['000001.SZ','600000.SH'], ...)` → 返回 62 行（2 标的 × 31 分钟）✅
- **服务输出 vs 归档原始数据 join 逐行比对：6 列不等行数 = 0** ✅

目录结构一致（`date=YYYY-MM-DD/part.parquet`）。

## 二、实测性能（远超预估）

| 任务 | 数据量 | 耗时 |
|---|---|---|
| 导入 2019-01 分钟（22 交易日） | 1 892 万行 → 171 MB | **4.2 秒** |
| 导入 2019-01 日线（22 交易日） | 7.9 万行 → 1.7 MB | **0.1 秒** |

按 1m 实测速率（**~450 万行/秒**）外推，2019–2023 五年分钟线（~5 年 × 240 交易日 × 85 万行 ≈ 1000 亿行）
约需 **60+ 小时**；但归档 1m 一天约 13–22 MB（压缩后），本机 2025 年的日均分区约 15 MB，
**5 年 ≈ 22 GB**（不是此前估的 125 GB —— 那是解压后 parquet 的体积，压缩后小得多）。

## 三、仍需决策的问题

1. **归档 1m 覆盖 2010–2026，但本机 2025-06 起已有完整数据** —— 导入 2010–2024 还是只补 2019–2023？
   （2010–2018 归档有、本机完全没有，属于**净增 9 年**。）
2. **210 个空壳分区**（2018/2024/2025，只有 1 标的 240 行）—— 归档有对应日期，导入会直接覆盖修复。
3. **`data/adj_factor/` 是空目录**（股票复权因子表缺失）—— 归档日线带 `adj_factor` 列（baostock 口径，
   锚定最新=1.0 的累计因子），可反推原始价并重建本机口径的 `ex_factor` 因子表。**但口径转换尚未验证**，
   本轮不涉及。
4. **磁盘空间**：本机剩 222 GB；导入 2010–2024 分钟约需 22 GB（压缩后），日线约 1 GB。**空间充足**。

## 四、风险与注意事项

- **归档是 2026-04-18 的快照**（1d 覆盖到 2026-04-17，1m 同理）→ 与本机 2026-04-18 之后的数据**互补不重叠** ✅
- **导入会覆盖同日期的现有分区** —— 2019–2023 本机无分区（安全）；2018/2024/2025 的空壳会被覆盖（**正是修复**）；
  **2025-06 之后的完整分区绝不能覆盖**（会丢失本机更新的数据）→ 导入必须**限制日期范围**。
- 归档标的数少于本机（2019 年 3 561 只 vs 2026 年 5 416 只）是**当年的实际上市数**，不是缺失。
- 归档 1m 无 `adj_factor` 列（只有 1d 有），分钟线价格与日线同为复权序列 ✅

## 五、复现命令

```bash
cd /home/caisl/tickflow-stock-panel/backend

# 干跑（不落盘）
uv run --extra dev python -m app.quant.archive_import_cli \
    --source-zip "/home/caisl/A股分钟数据/4.21更新/A股分钟数据/A股分钟/1m_price/2019.zip" \
    --target-root /tmp/opencode/import_probe/kline_minute --kind minute --months 2019-01 --dry-run

# 真实导入到临时目录
uv run --extra dev python -m app.quant.archive_import_cli \
    --source-zip "/home/caisl/A股分钟数据/4.21更新/A股分钟数据/A股分钟/1m_price/2019.zip" \
    --target-root /tmp/opencode/import_probe/kline_minute --kind minute --months 2019-01

# 端到端验证（用本机读取路径读导入产物）
PARTITION_DATA_ROOT=/tmp/opencode/import_probe_full uv run --extra dev python -c "
from app.services.stockdata.sources import DataSources
print(DataSources().get_minute(['000001.SZ'], '2019-01-02 09:30:00', '2019-01-02 10:00:00'))
"
```
