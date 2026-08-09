# 2026-08-09 ETF 真实单位净值接入（get_extras unit_net_value）

## 背景

wufu1.7 策略在 13:10 的动量计算里调用 `get_extras('unit_net_value', codes, ...)`，
用于计算 ETF 溢价率 `premium_rate = (etf_price - nav) / nav * 100`。

当前实现把 `unit_net_value` 用 `close` 收盘价近似：
- `premium_rate` 恒为 0，`passed_premium` 恒 True —— **溢价率过滤形同虚设**。
- 回测（`jqcompat.get_extras`）与模拟盘（`engine/jq/api.py.get_extras`）同为
  close 近似口径，两处一起失真。

**约束**：策略代码不可改动（策略要什么给什么，接口语义保持聚宽
`get_extras('unit_net_value', codes, start_date, end_date)` → DataFrame
行=日期、列=security）。

## 数据源选型（已实测）

| 源 | 净值能力 | 实测 |
|---|---|---|
| 腾讯 `qt.gtimg.cn` | ❌ 无净值 | 仅行情+PE/PB |
| 新浪 `hq.sinajs.cn` | ❌ 无净值 | 仅行情 |
| a-stock-data（47 端点） | ❌ 无基金净值端点 | README 端点清单 |
| tushare `fund_nav` | ✅ 有 unit_nav | 需 **2000 积分**（用户 200 分不够） |
| **akshare `fund_etf_fund_daily_em()`** | ✅ **真实单位净值** | **实测 1602 只全市场 ETF，免费零鉴权** |

`ak.fund_etf_fund_daily_em()` 一次返回全市场场内 ETF，列含：
`基金代码 / 基金简称 / 类型 / 单位净值 / 累计净值 / 增长率 / 市价 / 折价率`。
**无需建「场内→场外联接基金」映射表**。

注意：净值收盘后（基金公司披露）才有当日值；盘中只有昨日净值。策略用
`context.previous_date`（昨日）取数，正好匹配。

## 架构（对齐现有 stockdata 服务模式）

```
回源侧（stockdata 服务自治）
  etf_nav_service.sync_etf_nav(day)
    └─ ak.fund_etf_fund_daily_em() → {symbol: unit_nav}
    └─ 落盘 data/etf_nav/date=YYYY-MM-DD/part.parquet（按日幂等）
  三个触发点：
    1. 启动  backfill_to_now() 末尾        （missing 汇总加 etf_nav）
    2. 15:35 _run_sync 末尾                （同步当日）
    3. 00:00 scan_and_backfill_full()       （缺失日逐日补）

读取侧（StockDataClient 协议）
  sources.get_etf_nav(codes, date)  → 读 data/etf_nav 分区（对齐 get_daily）
  handlers.h_get_etf_nav           → 协议分发
  network_client.get_etf_nav(codes, date)
  get_extras('unit_net_value', ...) → jqcompat 与模拟盘 api 共用新逻辑
    └─ 按 code+date 从 StockDataClient 拉真实净值
    └─ 精确日优先 → 回退最近可用日 → 仍无则 warn + 空 DataFrame
```

## 第 1 节：回源侧

### 新文件 `backend/app/services/etf_nav_service.py`

对齐 `mootdx_service.py` 风格（logger、`_date`、幂等、`_append_failure`）。

- `ETF_NAV_ROOT = DATA_ROOT / "etf_nav"`（`date=YYYY-MM-DD/part.parquet`）。
- `sync_etf_nav(day=None) -> int`：
  - `day = day or _date.today()`；该日分区已存在 → 返回 0（幂等）。
  - 调 `ak.fund_etf_fund_daily_em()`，取 `基金代码`/`单位净值` 两列。
  - 转 polars DataFrame：`symbol`（JQ 格式 `510300.XSHG`）、`unit_nav` float、
    `date`（iso str）。
  - 原子写分区（临时文件 + rename，对齐 `_write_minute_partition`）。
  - 返回写入行数；失败记录 `_append_failure`。
- `_missing_etf_nav_days(now=None) -> list[_date]`：
  复用 `_market_closed` 口径（≥15:00 当日才算可回源），找出最新分区之后缺失的
  交易日；完全空分区补最近窗口。

### 触发点接线（修改 `mootdx_service.py` / `scheduler.py`）

1. `backfill_to_now()` 末尾：
   - `result["missing"]["etf_nav"] = {...latest/empty/missing...}`。
   - 对 `_missing_etf_nav_days()` 逐日 `sync_etf_nav(day)`，汇总
     `result["etf_nav_days"]`。
2. `scheduler._run_sync()`（15:35 cron）末尾：`etf_nav_service.sync_etf_nav()`。
3. `scan_missing_partitions()`：返回 dict 加 `"etf_nav"` 键
   （`_missing_days_in(calendar, ETF_NAV_ROOT)`）。
4. `backfill_missing_partitions()`：`missing["etf_nav"]` 逐日补
   `sync_etf_nav(day)`，汇总 `result["etf_nav_days"]`。

## 第 2 节：读取侧

### `sources.py`（DataSources）

`get_etf_nav(codes: list[str], date: str | None) -> pl.DataFrame`：
- 对齐 `get_daily`：`_scan_partitions("etf_nav", date, date, syms, cols)`
  + 短 TTL（`_HIST_TTL`）。
- cols：`["symbol", "unit_nav", "date"]`。
- `date=None` 时取最新分区。

### `handlers.py`

`h_get_etf_nav(p, s)`：`codes = _norm_codes(p["security"])`；
`date = p.get("date")`；返回 `"parquet", s.get_etf_nav(codes, date)`。
注册进 `HANDLERS`。

### `network_client.py`（StockDataClient）

`get_etf_nav(codes, date=None)`：`_request("get_etf_nav", {...})`，
`_parquet_to_dict` 解析，返回 `{code: {date: unit_nav}}` 或 DataFrame。

### `get_extras('unit_net_value', ...)`

新增共享实现（jqcompat 与模拟盘 `engine/jq/api.py` 都调用）：

```
def _get_etf_nav(securities, start_date, end_date):
    # 从 StockDataClient.get_etf_nav(codes, end_date) 拉真实净值
    # 精确 end_date 有 → 用；无 → 回退最近可用日
    # 仍无 → warn + 空 DataFrame
    # 返回 DataFrame：行=日期，列=security
```

- `jqcompat.get_extras`：`unit_net_value` 分支改用 `_get_etf_nav`。
- `engine/jq/api.py.get_extras`：`unit_net_value` 分支改用 `_get_etf_nav`
  （两者从各自环境取 DataManager/StockDataClient，但逻辑同一份）。

## 第 3 节：依赖与测试

### 依赖

`backend/pyproject.toml` 加 `akshare>=1.18,<2`（venv 已装 1.18.64）。

### 测试（`backend/tests/quant/`）

- `test_etf_nav_service.py`：
  - `sync_etf_nav` 幂等（mock akshare 返回 → 落盘 → 再调跳过）。
  - 落盘 schema（symbol/unit_nav/date）。
  - `_missing_etf_nav_days` 口径（收盘前不算缺失、收盘后算）。
  - `scan_missing_partitions` 含 `etf_nav` 键。
- `test_fix_datamanager.py` / 新增 `test_etf_nav_client.py`：
  - `DataSources.get_etf_nav` 读分区返回。
  - StockDataClient 往返（mock server 或现有测试基建）。
  - `get_extras('unit_net_value')` 返回真实净值（mock akshare / mock
    client），验证不再用 close 近似。

## 不做的事（YAGNI）

- 不建场内→场外映射表（`fund_etf_fund_daily_em` 直接按场内代码返回）。
- 不改策略代码。
- 不引入 tushare（200 积分不够 fund_nav）。
- 不清空历史净值的重建（从接入日往前按需回源）。
