# PTrade 原生化改造实施计划（wufu-v5.4 双持仓版）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把双持仓 PTrade 策略（`backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py`）的 jq→ptrade 转换层删除，正文直接调用原生 PTrade API，保持与 JQ 版完全对齐。

**Architecture:** 策略层删掉 ~535 行转换函数（`_pt`/`_cd`/`_BarUnit`/`_wide`/`_as_series_values`/`_positions_map`/`_pos_*`/`_safe_log`/`_warn`/`_debug`/`_update_universe`/`_get_total_value`/`_get_available_cash` 等），替换为 ~150 行原生辅助层；正文调用点改为直接使用 `get_history` 宽表/`get_positions`/`get_position`/`context.portfolio`/`set_universe`/`log`。引擎两侧（本地 `ptrade_api.py` 与 rqalpha `ptradecompat.py`）已保证格式，Task 1 用回归测试锁定。

**Tech Stack:** Python 3.x（uv）、pandas、pytest（`uv run --extra dev`）。

## Global Constraints

- 只改双持仓版 `wufu-v5.4-dual-adapt.ptrade.py`；单持仓版 `wufu-v5.4.ptrade.py` 不动。
- **交易逻辑一行不改**——只替换转换函数调用为原生写法。
- 验收硬指标：`uv run --extra dev pytest -m integration tests/quant/test_ptrade_vs_jq_alignment.py`（130 笔成交一致、日净值零差）。
- 策略仍须满足：无 f-string（`\bf(['"])` 正则不匹配）、无聚宽 API（jqdata/get_current_data/attribute_history/get_price(/record(/set_option 等）。
- 命令统一从 `backend/` 目录运行；测试用 `uv run --extra dev pytest`。
- ruff 规则：line-length 100，select E,F,I,N,UP,B,SIM,RUF，忽略 E501；中文注释文件已有 RUF001/2/3/RUF100 per-file-ignores。

---

### Task 1: 引擎 get_history 宽表格式回归测试

**Files:**
- Modify: `tests/quant/test_ptradecompat.py`（rqalpha 侧）
- Modify: `tests/quant/test_ptradeengine.py` 或 `tests/quant/test_sim_runner_flavor.py`（本地引擎侧，选已存在可注入 manager 的测试文件；若无则新建 `tests/quant/test_ptradeengine.py`）
- Test: 同上

**Interfaces:**
- Consumes: `app.quant.ptradecompat.get_history(count, frequency, field, security_list=...)`、`app.quant.ptradeengine.ptrade_api.get_history(...)`
- Produces: 两条回归断言——单标的 `get_history` 返回 `pd.DataFrame`（非 Series），且 `df[security]` 可取值（列名=标的码）

**背景：** 引擎两侧 get_history 已实现为「恒宽表、index=datetime、columns=PTrade 码」，Task 2 的策略改造（`df[code]` 取列）依赖此保证。本任务用回归测试锁定，防止后续回归。

- [ ] **Step 1: 写失败测试（rqalpha 侧）**

在 `tests/quant/test_ptradecompat.py` 追加（先确认文件现有测试结构再仿照）：

```python
def test_get_history_single_code_wide(monkeypatch):
    """单标的 get_history 返回宽表（非 Series），列名=标的码，可 df[code] 取值。"""
    from datetime import datetime
    import numpy as np
    import pandas as pd

    def _fake_batch(codes, count, freq, fields, end_dt):
        out = {}
        for c in codes:
            arr = np.zeros(count, dtype=np.dtype([("datetime", "S14"), ("close", "f8")]))
            for i in range(count):
                arr["datetime"][i] = "20260701093000"
                arr["close"][i] = 1.0 + i
            out[c] = arr
        return out

    import app.quant.ptradecompat as pc
    monkeypatch.setattr(pc, "_history_bars_batch", _fake_batch)
    df = pc.get_history(5, "1d", "close", security_list="510300.SS")
    assert isinstance(df, pd.DataFrame), "单标的必须返回 DataFrame"
    assert "510300.SS" in df.columns, "列名必须是标的码"
    assert len(df[df["510300.SS"] > 0]) > 0
```

- [ ] **Step 2: 运行确认（预期 PASS，锁定现有保证）**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradecompat.py::test_get_history_single_code_wide -v`
Expected: PASS（若 FAIL 说明格式保证有洞，修复 `ptradecompat.get_history` 返回宽表）

- [ ] **Step 3: 写失败测试（本地引擎侧）**

在 `tests/quant/test_ptradeengine.py`（若无则新建）追加：

```python
def test_get_history_single_code_wide_local():
    """本地引擎 ptrade_api.get_history 单标的返回宽表，列名=标的码。"""
    import pandas as pd
    from app.quant.ptradeengine import ptrade_api

    class _Fake:
        _daily_mem = {}
        _minute_mem = {}
        sources = {}

        def fetch(self, name, *a, **kw):  # noqa: N802
            idx = pd.date_range("2026-07-01", periods=6, freq="D")
            return pd.DataFrame({"close": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5]}, index=idx)

    from datetime import datetime
    ptrade_api._reset(_Fake(), 0.0001, 0.0001, 100000.0)
    ptrade_api._state["ctx"].current_dt = datetime(2026, 7, 4, 10, 0)
    df = ptrade_api.get_history(3, "1d", "close", security_list="510300.SS")
    assert isinstance(df, pd.DataFrame)
    assert "510300.SS" in df.columns
```

- [ ] **Step 4: 运行确认（预期 PASS）**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptradeengine.py -v`
Expected: PASS（若 FAIL 修复 `ptrade_api.get_history` 单标的返回宽表）

- [ ] **Step 5: Commit**

```bash
cd /home/caisl/tickflow-stock-panel && git add backend/tests/quant/test_ptradecompat.py backend/tests/quant/test_ptradeengine.py && git commit -m "test: 锁定 ptrade get_history 单标的宽表格式保证（列名=标的码）"
```

---

### Task 2: 策略重写为原生 PTrade

**Files:**
- Modify: `tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py`
- Test: `tests/quant/test_ptrade_strategy_file.py`、`tests/quant/test_ptrade_vs_jq_alignment.py`

**Interfaces:**
- Consumes: 引擎保证的格式——`get_history` 恒宽表（columns=PTrade 码）、`get_positions()` 恒 `{码: Position}`（amount>0）、`get_position(code)` 无持仓返回空 Position、Position 字段 `amount/enable_amount/cost_basis/last_sale_price`、handle_data 的 `data` 为 `{码: obj}`（含 close/price/volume/money）
- Produces: 原生 PTrade 风格策略文件（保留 `_current_dt`/`_today`/`_BARS`/`_price` 等少量精简原生辅助，删掉全部 jq 转换函数）

**规则：** 平台辅助层整体替换（原文件第 40–575 行 → 下方新代码）；正文按下方「调用点迁移」逐一替换；交易逻辑函数（`check_a_share_weak_period`/`select_cross_asset_dual`/`execute_buy_trades`/`execute_sell_trades`/`smart_order_target_value`/`minute_level_stop_loss` 等）内部判断与流程**一字不改**，仅替换其中对转换函数的调用。

- [ ] **Step 1: 更新文件头注释**

把第 15–28 行「平台差异适配说明」整段替换为：

```python
# 平台差异适配说明（聚宽 → PTrade / 国金版本）：
#   - 代码格式：.SS / .SZ（策略内直接用 PTrade 码，无转换函数）
#   - 全局状态 g.* ：PTrade 同样支持并自动持久化
#   - 调度：晨间 → before_trading_start；盘中 09:40/13:10/13:10/13:10 → run_daily；
#           收盘重置 → after_trading_end；分钟止损 → handle_data
#   - 日线历史：get_history(count, '1d', field, security_list, fq='pre')，返回宽表
#             （index=时间, columns=代码），单标的也返回 DataFrame，可用 df[code] 取列
#   - 盘中数据：PTrade 无 get_current_data()，由 handle_data 的 data 参数捕获到 _BARS；
#             SecurityUnitData 含 dt/open/high/low/close/price/volume/money；
#             停牌用 get_stock_status(query_type='HALT')（_is_halted），涨跌停价用日线 high_limit/low_limit 字段（_limit_prices）
#   - 持仓：get_position(sec) 返回 Position（amount / enable_amount / cost_basis / last_sale_price）
#   - 现金/总资产：context.portfolio.cash / .portfolio_value（PTrade 无 get_cash）
#   - 动态 ETF 池：用 get_market_list()/get_market_detail() 枚举全市场基金，取不到时优雅降级为固定池；
#     全市场 6000+ 标的的成交额查询按 200 只分块（_get_money_avg_series），阈值按实际可交易池估算，避免回测挂起
#   - record()/log.set_level/set_option 等聚宽独有 API 已移除；日志用 log.info/warn/error/debug
```

- [ ] **Step 2: 替换平台辅助层**

把原文件第 40 行（`# ==================== 日志兼容层...`）到第 575 行（`_ensure_fund_universe` 结束）整段替换为：

```python
# ==================== 平台辅助层（原生 PTrade API） ====================
_BARS = {}  # 最新行情快照 {code: SecurityUnitData}，由 handle_data/before_trading_start 捕获


def _current_dt(context):
    try:
        return context.blotter.current_dt
    except Exception:
        return datetime.now()


def _today(context):
    return _current_dt(context).date()


def _capture_bars(data):
    global _BARS
    _BARS = data or {}


def _price(security, context):
    """当前价：快照 close/price → 当日最新分钟收盘 → 最近日线收盘。"""
    obj = _BARS.get(security)
    p = (getattr(obj, 'close', 0) or getattr(obj, 'price', 0) or 0) if obj else 0
    if p:
        return float(p)
    try:
        mdf = get_history(1, '1m', 'close', security_list=security, include=True)
        if mdf is not None and security in mdf.columns and len(mdf):
            val = float(mdf[security].values[-1])
            if val == val:  # not NaN
                return val
    except Exception:
        pass
    try:
        ddf = get_history(1, '1d', 'close', security_list=security, include=True)
        if ddf is not None and security in ddf.columns and len(ddf):
            return float(ddf[security].values[-1])
    except Exception:
        pass
    return 0


# ==================== 停牌 / 涨跌停价（get_stock_status / 日线字段，按日缓存） ====================
_HALT_CACHE = {}
_LIMIT_CACHE = {}


def _refresh_halt_status(codes, context):
    global _HALT_CACHE
    today = _today(context).strftime('%Y%m%d')
    if today not in _HALT_CACHE:
        result = {}
        CHUNK = 100
        for i in range(0, len(codes), CHUNK):
            try:
                res = get_stock_status(list(codes)[i:i + CHUNK], query_type='HALT', query_date=today)
                if res:
                    result.update(res)
            except Exception:
                continue
        _HALT_CACHE[today] = result
    return _HALT_CACHE[today]


def _is_halted(code, context):
    """停牌检测（get_stock_status HALT，按日缓存）。失败默认 False，不误判停牌。"""
    try:
        m = _HALT_CACHE.get(_today(context).strftime('%Y%m%d'))
        if m is None:
            m = _refresh_halt_status([code], context)
        return bool(m.get(code))
    except Exception:
        return False


def _single_daily(code, field, context):
    """单标的最近日线字段值（get_history 宽表取列）。"""
    try:
        df = get_history(1, '1d', field, security_list=code, include=True)
        if df is not None and code in df.columns and len(df):
            return float(df[code].values[-1])
    except Exception:
        pass
    return None


def _limit_prices(code, context):
    """当日涨跌停价 (high, low)。失败返回 (None, None) 由调用方跳过限制判断。"""
    today = _today(context).strftime('%Y%m%d')
    key = (today, code)
    if key in _LIMIT_CACHE:
        return _LIMIT_CACHE[key]
    high = _single_daily(code, 'high_limit', context)
    low = _single_daily(code, 'low_limit', context)
    _LIMIT_CACHE[key] = (high, low)
    return (high, low)


def get_security_name(security):
    """标的名称：动态池名称缓存 → get_stock_name → 代码兜底。"""
    try:
        if getattr(g, 'etf_names_dict', {}) and security in g.etf_names_dict:
            return g.etf_names_dict[security]
        d = get_stock_name(security)
        if d and d.get(security):
            return d.get(security)
    except Exception:
        pass
    return security


def _get_today_volumes(context, codes):
    """当日累计成交量（分钟线求和，分块避免超大查询挂起）。失败返回 {}。"""
    out = {}
    today = _today(context)
    CHUNK = 100
    for i in range(0, len(codes), CHUNK):
        chunk = list(codes)[i:i + CHUNK]
        try:
            mdf = get_history(241, '1m', 'volume', security_list=chunk, include=True)
            if mdf is None or mdf.empty:
                continue
            for code in chunk:
                if code not in mdf.columns:
                    continue
                s = mdf[code]
                if hasattr(mdf.index, 'date'):
                    s = s[mdf.index.date == today]
                s = pd.to_numeric(s, errors='coerce').dropna()
                out[code] = float(s.sum())
        except Exception:
            continue
    return out


def _get_money_avg_series(codes, count, context, field='money'):
    """分块 get_history 拉取成交额并计算日均，返回 pd.Series(code -> 日均成交额)。
    避免对上千只标的单次 get_history 查询导致回测挂起。
    field='money_corrected'：返回引擎修正后的元成交额（对齐聚宽 get_daily_money_cached
    口径，用于流动性阈值）；真 PTrade 无该字段，get_history 回退 'money'。"""
    result = pd.Series(dtype=float)
    CHUNK = 200
    for i in range(0, len(codes), CHUNK):
        chunk = list(codes)[i:i + CHUNK]
        try:
            df = get_history(count, '1d', field, security_list=chunk)
            if df is None or df.empty:
                continue
            df = df.fillna(0.0)
            avg = df.sum(axis=0) / count
            for code in chunk:
                if code in avg.index:
                    result[code] = float(avg[code])
        except Exception:
            continue
    return result


def _get_money_daily_totals(codes, context):
    """按日汇总样本池成交额，返回 {日期: (总成交额, 有成交只数)}，失败返回 None。"""
    try:
        CHUNK = 200
        totals = {}
        for i in range(0, len(codes), CHUNK):
            chunk = list(codes)[i:i + CHUNK]
            df = get_history(3, '1d', 'money', security_list=chunk)
            if df is None or df.empty:
                continue
            df = df.fillna(0.0)
            for day, row in df.iterrows():
                key = day.date() if hasattr(day, 'date') else day
                m, cnt = totals.get(key, (0.0, 0))
                totals[key] = (m + float(row.sum()), cnt + int((row > 0).sum()))
        return totals
    except Exception:
        return None


# ==================== 全市场基金枚举（动态池用，尽力实现+优雅降级） ====================
def _get_all_fund_codes():
    """枚举全市场基金代码/名称 {code: name}。
    通过 get_market_list() 遍历所有市场，get_market_detail(mic) 拉取产品。
    失败返回 None（调用方降级）。"""
    try:
        ml = get_market_list()
        if ml is None:
            return None
        fund_codes = {}
        for _, r in ml.iterrows():
            mic = r.get('finance_mic') or r.get('market_code') or r.get('code') or r.get('market')
            if not mic:
                continue
            try:
                detail = get_market_detail(mic)
            except Exception:
                continue
            if detail is None or detail.empty:
                continue
            cols = list(detail.columns)
            pc_col = 'prod_code' if 'prod_code' in cols else ('code' if 'code' in cols else None)
            pn_col = 'prod_name' if 'prod_name' in cols else ('name' if 'name' in cols else None)
            if not pc_col:
                continue
            for _, drow in detail.iterrows():
                try:
                    pc = str(drow[pc_col])
                    if pc in fund_codes:
                        continue
                    base = pc.split('.')[0]
                    if not (len(base) == 6 and base.isdigit()):
                        continue
                    fund_codes[pc] = str(drow[pn_col]) if pn_col else pc
                except Exception:
                    continue
        if not fund_codes:
            return None
        return fund_codes
    except Exception as e:
        log.warn('枚举全市场基金失败: %s' % e)
        return None


def _ensure_fund_universe():
    """缓存全市场基金表 g._fund_universe（{code: name}），失败则空表"""
    if getattr(g, '_fund_universe', None) is None:
        fc = _get_all_fund_codes()
        g._fund_universe = fc if fc else {}
    return g._fund_universe
```

- [ ] **Step 3: 调用点迁移（initialize / 钩子）**

逐条应用（`行号`为原文件行号，改完行号会变，按内容匹配）：

| 原代码 | 新代码 |
|---|---|
| `_warn('设置佣金失败(仅回测有效): %s' % e)` | `log.warn('设置佣金失败(仅回测有效): %s' % e)` |
| `_warn('设置滑点失败(仅回测有效): %s' % e)` | `log.warn('设置滑点失败(仅回测有效): %s' % e)` |
| `g.global_etf_pool = [_pt(c) for c in g.global_etf_pool]` | 删除该行（池内已是 .SS/.SZ） |
| `g.china_etf_pool = [_pt(c) for c in g.china_etf_pool]` | 删除该行 |
| `g.defensive_etf = _pt("511880.XSHG")  # 银华日利 货币ETF` | `g.defensive_etf = "511880.SS"  # 银华日利 货币ETF` |
| `_update_universe(g.fixed_etf_pool)`（initialize 内） | `set_universe(g.fixed_etf_pool)` |
| `_set_last_data(data, context)`（before_trading_start 内） | `_capture_bars(data)` |
| `_set_last_data(data, context)`（handle_data 内） | `_capture_bars(data)` |
| `_update_universe(g.merged_etf_pool)`（afternoon_routine 内） | `set_universe(g.merged_etf_pool)` |
| `_update_universe(g.merged_etf_pool)`（daily_merge_etf_pools 内） | `set_universe(g.merged_etf_pool)` |

注意：`g.global_etf_pool` 与 `g.china_etf_pool` 列表内容（第 592–716 行）保持原样，仅删掉上面的 `_pt` 转换行。**必须确认池内代码已是 .SS/.SZ 格式**（原文件池里是 .XSHG/.XSHE，删除转换行后需把池内所有 `.XSHG`→`.SS`、`.XSHE`→`.SZ`；可用整文件替换确认，但注意 `_get_all_fund_codes` 内的 `.SH`/`.SZ` 判断与 `to_pt` 无关、不受影响）。

- [ ] **Step 4: 调用点迁移（check_positions / monitor_drawdown）**

`check_positions`（原 949–961 行）：

```python
def check_positions(context):
    try:
        for security, position in get_positions().items():
            security_name = get_security_name(security)
            log.info("📊 【持仓检查】%s %s, 数量: %d, 成本: %.3f, 当前价: %.3f" % (
                security, security_name,
                int(position.amount), position.cost_basis, position.last_sale_price))
            if _is_halted(security, context):
                log.info("⚠️ %s %s 今日停牌" % (security, security_name))
    except Exception as e:
        log.warn("【持仓检查】执行异常: %s" % e)
```

`monitor_drawdown`（原 963–989 行）内 3 处替换：
- `current_value = _get_total_value(context)` → `current_value = context.portfolio.portfolio_value`
- `for security, position in _positions_map().items():` → `for security, position in get_positions().items():`
- `positions_info.append("%s:%d股" % (security_name, int(_pos_amount(position))))` → `positions_info.append("%s:%d股" % (security_name, int(position.amount)))`

- [ ] **Step 5: 调用点迁移（阈值 / 池过滤 / 动态池）**

`calculate_global_etf_threshold`（原 992–1028 行）：
- `trade_days = _last_n_trade_days(3)` → `trade_days = get_trade_days(end_date=get_trading_day(-1), count=3)`
- 所有 `_warn(...)` → `log.warn(...)`（共 4 处）

`filter_global_pool_by_volume`：所有 `_warn(...)` → `log.warn(...)`（3 处）。

`update_sector_pool`：`_warn(...)` → `log.warn(...)`（3 处，行 1119/1125/1196）。

`filter_fixed_pool_by_volume`：`_warn(...)` → `log.warn(...)`（3 处，行 1288/1307 + 1286 处 `_warn("【固定池过滤】无法获取成交额数据，跳过过滤")`）。

`calculate_and_log_ranked_etfs`：`_warn(...)` → `log.warn(...)`（1 处）。

- [ ] **Step 6: 调用点迁移（动量 / 走弱期 / 排名）**

`calculate_all_metrics_for_etf`：`_debug(...)` → `log.debug(...)`（1 处）。

`check_a_share_weak_period`（原 1434–1517 行）：
- `closes = _as_series_values(df)` → `closes = df[code].values`
- `_warn(...)` → `log.warn(...)`（1 处）

`get_final_ranked_etfs`（原 1553–1714 行）：
- 删除 `current_data = _cd()`（未使用）
- `close_df = _wide(get_history(safe_lookback, '1d', 'close', security_list=etf_set, fq='pre'))` → `close_df = get_history(safe_lookback, '1d', 'close', security_list=etf_set, fq='pre')`
- `volume_df = _wide(get_history(safe_lookback, '1d', 'volume', security_list=etf_set))` → `volume_df = get_history(safe_lookback, '1d', 'volume', security_list=etf_set)`
- 删除 `obj = current_data.get(etf)`（未使用）
- `_warn(...)`/`_debug(...)` → `log.warn(...)`/`log.debug(...)`（共 5 处）
- `current_price = _current_price(etf, context)` → `current_price = _price(etf, context)`
- `current_holdings = list(_positions_map().keys())` → `current_holdings = list(get_positions().keys())`

- [ ] **Step 7: 调用点迁移（买卖 / 止损）**

`execute_sell_trades`：
- `current_positions = _positions_map()` → `current_positions = get_positions()`
- `if _pos_amount(position) > 0 and security not in target_set:` → `if position.amount > 0 and security not in target_set:`

`execute_buy_trades`：
- `current_positions = _positions_map()` → `current_positions = get_positions()`
- `remaining_cash = _get_available_cash(context)` → `remaining_cash = context.portfolio.cash`
- `total_value = _get_total_value(context)` → `total_value = context.portfolio.portfolio_value`

`is_temporarily_suspended`：
- `vals = _as_series_values(minute_data)` → `vals = minute_data[security].values`
- `_debug(...)` → `log.debug(...)`

`smart_order_target_value`（原 1937–2009 行）：
- `price = _current_price(security, context)` → `price = _price(security, context)`
- `max_shares = int(_get_available_cash(context) / estimated_price)` → `max_shares = int(context.portfolio.cash / estimated_price)`
- `cur_pos = _get_position(security)` → `cur_pos = get_position(security)`
- `cur_amount = _pos_amount(cur_pos)` → `cur_amount = cur_pos.amount`
- `closeable = _pos_avail(cur_pos)` → `closeable = cur_pos.enable_amount`
- `_warn("下单失败: ...")` → `log.warn("下单失败: ...")`

`minute_level_stop_loss`（原 2012–2057 行）：
- `for security, position in _positions_map().items():` → `for security, position in get_positions().items():`
- `if _pos_amount(position) <= 0 or _pos_avail(position) <= 0:` → `if position.amount <= 0 or position.enable_amount <= 0:`
- `current_price = _current_price(security, context)` → `current_price = _price(security, context)`
- `cost_price = _pos_cost(position)` → `cost_price = position.cost_basis`

`check_defensive_etf_available`（原 2060–2079 行）开头替换：

```python
    defensive_etf = g.defensive_etf
    obj = _BARS.get(defensive_etf)
    if obj is None:
        return False
    price = getattr(obj, 'close', 0) or getattr(obj, 'price', 0) or 0
```

- [ ] **Step 8: 删除死代码**

删除原 1535–1550 行 `def _get_today_volume(context, security):` 整个函数（无任何调用）。

- [ ] **Step 9: 全局自查 + 编译**

```bash
cd backend && uv run --extra dev python -c "import py_compile; py_compile.compile('tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py', doraise=True); print('compile OK')"
```

自查确认**无残留**转换函数调用（输出应为空）：

```bash
cd backend && rg -n "_pt\(|_cd\(|_cd_field|_set_last_data|_safe_log|_warn\(|_debug\(|_wide\(|_as_series_values|_positions_map|_get_position\(|_pos_amount|_pos_avail|_pos_cost|_pos_price|_get_total_value|_get_available_cash|_update_universe|_current_price\(|_get_today_volume\(|_last_n_trade_days|_previous_trading_day" tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py
```

Expected: 无输出（除注释外）。若 `_get_today_volume` 残留说明死代码未删。

- [ ] **Step 10: 单元测试**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptrade_strategy_file.py tests/quant/test_ptradecompat.py tests/quant/test_ptradeengine.py tests/quant/test_sim_runner_flavor.py -q`
Expected: 全 PASS（若 `test_dual_position_config` 因 `run_daily(context, ...)` 断言失败——该断言要求 3 行 `run_daily(context, X, time='13:10')`，原样保留即可）

- [ ] **Step 11: 对齐门禁（硬指标）**

Run: `cd backend && uv run --extra dev pytest -m integration tests/quant/test_ptrade_vs_jq_alignment.py -q`
Expected: PASS（130 笔成交一致、日净值零差）。**若 FAIL：对比交易/净值差异，定位是格式切片回归还是逻辑改动，修复后重跑。绝不允许为通过门禁而放松断言。**

- [ ] **Step 12: Commit**

```bash
cd /home/caisl/tickflow-stock-panel && git add backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py && git commit -m "refactor: ptrade 双持仓策略去 jq 转换层，正文直调原生 PTrade API（对齐门禁通过）"
```

---

### Task 3: 加固转换层删除断言

**Files:**
- Modify: `tests/quant/test_ptrade_strategy_file.py`
- Test: 同上

**Interfaces:**
- Consumes: Task 2 产出的新策略文件
- Produces: 回归断言——可执行代码中不存在任何 jq→ptrade 转换函数

- [ ] **Step 1: 写失败测试**

在 `tests/quant/test_ptrade_strategy_file.py` 追加：

```python
_CONV_LAYER = ["_pt(", "_cd(", "_cd_field", "_set_last_data", "_BarUnit", "_safe_log",
               "_warn(", "_debug(", "_wide(", "_as_series_values", "_positions_map",
               "_get_position(", "_pos_amount", "_pos_avail", "_pos_cost", "_pos_price",
               "_get_total_value", "_get_available_cash", "_update_universe", "_current_price(",
               "_get_today_volume("]


def test_no_conversion_layer():
    """jq→ptrade 转换层已删除：可执行代码不得出现任何转换函数调用。"""
    for line in _code_lines():
        for kw in _CONV_LAYER:
            assert kw not in line, kw
```

- [ ] **Step 2: 运行确认（Task 2 后应 PASS）**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_ptrade_strategy_file.py::test_no_conversion_layer -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
cd /home/caisl/tickflow-stock-panel && git add backend/tests/quant/test_ptrade_strategy_file.py && git commit -m "test: 断言 ptrade 策略无 jq 转换层函数残留"
```

---

### Task 4: 同步更新 store 策略文件

**Files:**
- Modify: `data/quant_strategies/dual_v54_ptrade.py`（gitignored，不入库）
- Test: 无（用命令验证）

**Interfaces:**
- Consumes: Task 2 产出的新策略文件
- Produces: store 中 `dual_v54_ptrade` 更新为新原生版（模拟盘账户 691605d0 下次重启即用）

- [ ] **Step 1: 保存新策略到 store**

Run:

```bash
cd backend && uv run --extra dev python -c "
from pathlib import Path
from app.quant.strategies.store import save_strategy
code = Path('tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py').read_text(encoding='utf-8')
save_strategy('dual_v54_ptrade', '五福v5.4双持仓(ptrade)', code)
print('saved, len=', len(code))
"
```

Expected: `saved, len=...`

- [ ] **Step 2: 验证 store 策略已更新**

Run:

```bash
cd backend && uv run --extra dev python -c "
from app.quant.strategies.store import get_strategy
s = get_strategy('dual_v54_ptrade')
code = s['code']
assert 'get_positions()' in code and '.SS' in code
for bad in ('_pt(', '_cd(', '_wide(', '_positions_map', '_warn('):
    assert bad not in code, bad
print('store strategy is native-ptrade OK')
"
```

Expected: `store strategy is native-ptrade OK`

---

### Task 5: 全量验证

**Files:** 无（纯验证）

- [ ] **Step 1: ruff**

Run: `cd backend && uv run --extra dev ruff check app tests`
Expected: 无错误

- [ ] **Step 2: 全量单测**

Run: `cd backend && uv run --extra dev pytest tests/quant -q`
Expected: 全 PASS（已知排除：`tests/quant/...` 中若有 pre-existing 失败需与本任务无关——本改造前跑过一次全绿，除 `test_runner_dingtalk.py` 在 custom-main 已存在失败，不在本任务范围）

- [ ] **Step 3: 对齐门禁（再次确认）**

Run: `cd backend && uv run --extra dev pytest -m integration tests/quant/test_ptrade_vs_jq_alignment.py -q`
Expected: PASS

- [ ] **Step 4: 回测冒烟（前端链路）**

Run:

```bash
cd backend && uv run --extra dev python -c "
from app.quant import service
rid = service.submit_backtest({'strategy_id': 'dual_v54_ptrade', 'name': '原生化冒烟', 'start': '2026-07-10', 'end': '2026-07-14', 'capital': 100000.0, 'fee': 0.0001, 'slippage': 0.0001})
print('run_id:', rid)
"
```

然后（在 backend/ 下另起命令，用 `setsid` 后台跑，`sleep 60` 后查状态）：

```bash
cd backend && setsid uv run --extra dev python scripts/run_quant_backtest.py <run_id> > /tmp/native_smoke.log 2>&1 </dev/null & disown
```

Expected: run 状态 `done`，metrics 正常（`uv run --extra dev python -c "from app.quant import db; r = db.get_run('<run_id>'); print(r['status'])"` → `done`）

- [ ] **Step 5: Commit（如有未提交改动）**

Run: `cd /home/caisl/tickflow-stock-panel && git status --short`
Expected: 干净（若 store 策略等有改动，确认不入库——`data/` 整体 gitignored）
