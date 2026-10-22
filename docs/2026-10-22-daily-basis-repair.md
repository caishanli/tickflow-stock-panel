# 日线落盘复权口径修复（2026-10-22）

## 结论（一句话）

`data/kline_daily` / `data/kline_etf_daily` 的存储契约是**不复权原始价**（读侧
`DataSources.apply_qfq_daily` 才乘 `ex_factor`）。但回源写盘复用了 `get_daily` 的默认
**前复权换算**，导致**事后批量回补**写出的历史日被多复权一次；读侧再乘因子 =
**重复复权**。已修复数据 + 修复根因 + 加静态守卫测试。

## 症状

- `get_price(fq='pre')` 返回的前复权价明显偏低：

  | 标的 / 日期 | baostock 前复权真值 | 修复前服务端返回 | 偏差 |
  |---|---|---|---|
  | 000001.SZ 2023-05-04 | 10.2091 | 8.3693 | **-18%** |
  | 688032.SH 2023-05-04 | 234.8337 | 98.5348 | **-58%** |
  | 600000.SH 2023-05-04 | 6.5696 | 5.6192 | **-14.5%** |
  | 300750.SZ 2023-05-04 | 209.9427 | 195.3814 | -7% |
  | 000001.SZ 2025-09-15 | 10.8069 | 10.8069 | ✓ |

- 日线收盘价与同日分钟线最后一根收盘价对不上（2023 年 54% 的标的），
  分钟线是原始价、日线被复权过。
- 2023-04-19 → 2023-12-20 的日线序列与相邻年份不连续（假跳变）。

## 根因

两条链路叠加：

1. `MootdxSource.get_daily`（`app/quant/jqengine/datasource/mootdx_src.py`）取到通达信
   原始价后，用 `_to_qfq` 按 xdxr 除权事件换算**前复权**。
2. `_to_qfq` 计算除权因子时，分子的"昨收"取的是**帧内前一行收盘价**
   （`close.loc[close.index < ex_dt]`）。而 `mootdx_service.sync_daily` 是
   **单日帧**（`get_daily(sym, day, day)`）：

   - **当日增量写**（帧内没有该日之后的除权事件）→ 因子不生效 → 恰好是原始价 ✓
   - **事后批量回补**（帧内有未来除权事件）→ 写成前复权 ✗

   而且多个后续事件都拿同一个"当天收盘"当昨收，误差会累积
   （688032 差 0.5%、000001 差 2.2%）。

所以**每日收盘 cron 写的数据是对的**，只有"回补历史"写进去的是错的。实测污染范围：

- 股票日线：`2023-04-19 → 2023-12-20`（165 个交易日，连续），抽样 250 只 × 860 日里
  该段 73% 的 (标的,日) 是前复权；2024 起 100% 原始价。
  （这 165 天正是本项目最早采集数据的那段，mtime 显示 2026-09-08 一次批量回补写出。）
- ETF 日线：同一批回补写了整个历史，抽样 60 只里 7 只（3.5% 单元格）被污染，
  其余 ETF 因"该日之后无除权事件"而恰好等于原始价。

## 修复

### 1. 数据修复（工具 `app/quant/daily_basis_repair.py`）

```bash
cd backend
# 检测（抽样比对 baostock 不复权价，无副作用）
uv run --extra dev python -m app.quant.daily_basis_repair \
  --detect --from 2023-04-19 --to 2023-12-20 --samples 250

# 股票：用归档（baostock 原始价，逐行精确）重写这 165 个分区
uv run --extra dev python -m app.quant.daily_basis_repair \
  --repair --apply --from 2023-04-19 --to 2023-12-20

# ETF：用 baostock 不复权日线只修正价格列（不删行、不改 volume/amount）
uv run --extra dev python -m app.quant.daily_basis_repair \
  --etf-detect --from 2016-01-01 --to 2026-10-21 --samples 60
uv run --extra dev python -m app.quant.daily_basis_repair \
  --etf-repair --apply --from 2016-01-01 --to 2026-10-21
```

安全策略：改前备份到 `data/.daily_basis_backup/`（股票 165 个、ETF 在 `etf/` 子目录）、
原子写 + `daily_partition_lock`、**归档是本机严格超集（本机独有标的 0 行）**，
ETF 只替换 `|本机/baostock-1| > 0.5%` 的 OHLC 单元格。

**残留清扫**（另一条 qfq 链路写进来的零散单元格）：`alt_daily` 的腾讯 qfq 补洞
路径也曾把 qfq 价写进日线（2025-07-31 → 2026-09-09 共 270 个分区，仅"之后有除权
事件"的标的受影响）。用本地分钟线（原始价）预筛可疑标的，再用 baostock 原始价确认：

```bash
uv run --extra dev python -m app.quant.daily_basis_repair \
  --baostock-repair --apply --from 2025-07-31 --to 2026-09-09
```

**派生表必须同步重建**（`kline_daily_enriched` 用 `adj_factor` 现算 close/raw_close，
不重建就还是旧价）：删掉受影响日期的 enriched 分区（含空目录）后跑
`run_pipeline(new_dates_only=True)` 即可增量补齐（165 天 ≈ 6 秒）。

### 2. 根因修复（防止再次发生）

- `MootdxSource.get_daily(..., *, qfq: bool = True)` 新增开关；
- `_guarded_get_daily(..., *, qfq=True)` 透传；
- **`sync_daily` 的股票/ETF 取数显式传 `qfq=False`**（回测桥默认 `qfq=True` 不变）。

### 3. 守卫测试

`backend/tests/quant/test_daily_basis_contract.py`：

1. `get_daily(qfq=False)` 不调 `_to_qfq`、原样返回原始价；
2. 默认（`qfq=True`）仍换算，回测桥行为不变；
3. `_guarded_get_daily` 正确透传 `qfq`；
4. **AST 静态守卫**：`mootdx_service.sync_daily` 里的 `_guarded_get_daily(...)` 必须带
   `qfq=False` —— 谁把它改回默认值，测试直接失败。

## 修复后验证（实测）

| 项目 | 修复前 | 修复后 |
|---|---|---|
| `--detect` 抽样（250 只 × 165 日） | 原始价占比 27% | **`raw_frac = 1.0`、`qfq_days = 0`** |
| `get_price(fq='pre')` vs baostock 前复权（6 用例） | 偏 7%~58% | **ratio = 1.000000（全部）** |
| 日线 close vs 分钟最后一根 close（2023 年 4 天 + 2024 对照） | 54% 不一致 | **0.00%（最大差 0.0000）** |
| 污染段边界收益率（2023-04-18→19、2023-12-20→21） | 假跳变 | **与 baostock 前复权收益率 100% 一致（最大偏差 0.000000）** |
| 复权伪跳变审计（`adj_factor_audit.py`，500 只） | — | 17 只全为 ETF 因子表伪跳变（既有隔离名单问题，与价格口径无关） |

**全库终检**（120 只随机标的 × 2016-01-01..2026-10-21，共 240 957 个 (标的,日) 单元格，
比对 baostock 不复权价）：

| 年份 | 原始价一致率 |
|---|---|
| 2016 ~ 2025 | **100.0000%**（每年） |
| 2026 | **100.0000%** |
| 合计 | **240 957 / 240 957 = 100.0000%** |

ETF 域 `--etf-detect` 抽样 60 只 × 41 296 单元格：修复前 1 461 个不一致 → 修复后 **0**。

## 运维注意

**改完 `mootdx_service` 必须重启后端**（reload 已关闭）：`sync_daily` 跑在
stockdata 服务进程里，由主后端 `stockdata_guardian` 托管 —— 重启主后端即重启它。
未重启前，下一次"回补历史"仍会写成前复权价（当日增量写不受影响）。

2026-10-22 10:23 已重启 stockdata 服务（pid 598574 → 759049，8 秒内恢复；
盘中实测取数正常，9 个模拟盘进程存活）。今后只重启该服务即可：
`kill $(ss -tlnp | grep :3322 | grep -oP 'pid=\K[0-9]+')`，守护进程 3s 自愈。

## 仍未处理（明确记录）

1. **ETF 前复权因子表只覆盖 2025-06-30 起**（`data/adj_factor_etf/all.parquet`）→
   `get_price(fq='pre')` 对 ETF 2025-06-30 之前的日期只能用"首因子"近似
   （实测 510300.SH 2023-06-15 偏高 4.4%）。修法：用 baostock 的 raw 与 qfq 两条
   价格序列直接反推逐日因子（`factor = qfq/raw`，锚定最新=1.0），每只 ETF 2 次查询
   ≈ 3500 次 / 约 40 分钟。
2. `data/adj_factor/all.parquet` **不存在**（`indicators/pipeline.py` 读它）→
   `kline_daily_enriched.close == raw_close`（派生表 / 前端展示为原始价）。
   量化侧走 `adj_factor_baostock` 不受影响。
3. 数据源固有、非缺陷：2016-2018 分钟 `volume=0` 占位行约 10%（2020+ 约 5%）、
   当日无行情的标的、因子表缺 356 只（北交所/退市）、284 行 >100% 跳变多为新股/
   北交所改号。

## 相关

- 归档导入的记录见 `docs/2026-10-20-archive-import-feasibility.md`
- 因子表权威源：`data/adj_factor_baostock/all.parquet`（股票，锚定最新=1.0）
- 口径真值对照脚本：`/tmp/opencode/probe_basis.py`、`probe_e2e.py`
