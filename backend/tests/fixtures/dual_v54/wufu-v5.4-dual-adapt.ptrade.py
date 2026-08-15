# -*- coding: utf-8 -*-
# ============================================================
# 【五福闹新春】v5.4（双持仓自适应版）— PTrade 移植版
# 双持仓逻辑移植自（聚宽 JoinQuant）：backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.py
# 单持仓 ptrade 底座：backend/tests/fixtures/wufu_v54/wufu-v5.4.ptrade.py
# 克隆自聚宽文章：https://www.joinquant.com/post/74243
# 作者：烟花三月ETF
# v5.3（本地改进版）：A1 盈利保护止损 / A2 持仓宽容（调仓惰性）/ A3 走弱期退出确认，均可独立开关。
# v5.4（胜率导向）：D1 锁盈止损（保护线 1.0→成本×X） / D2 买入过滤收紧 / D3 高位回落止盈，均可独立开关。
# v5.4 优化（2026-08-12）：A3 退出均线 20→15（弱市反弹更快回补 A 股池，避免错过反弹被锁在全球池）。
# v5.4 双持仓（2026-08-14）：holdings_num=2；第 4 步跨资产双持仓选择 select_cross_asset_dual()
#   （slot0=全池动量第一大权重，slot1=另一资产大类动量第一弱腿，自适应权重 0.5~0.85）；
#   买入按 target_weights 槽位目标市值分配。13:10 调度对齐模拟盘口径。
#
# 平台差异适配说明（聚宽 → PTrade / 国金版本）：
#   - 代码格式：.XSHG → .SS  /  .XSHE → .SZ（策略内用 _pt() 自动转换）
#   - 全局状态 g.* ：PTrade 同样支持并自动持久化
#   - 调度：晨间 → before_trading_start；盘中 09:40/13:10/13:10/13:10 → run_daily；
#           收盘重置 → after_trading_end；分钟止损 → handle_data
#   - 日线历史：get_history(count, '1d', field, security_list, fq='pre')；多标的返回格式随内置 Python 版本而异（_wide 兼容）
#   - 盘中数据：PTrade 无 get_current_data()，由 handle_data 的 data 参数捕获快照（_set_last_data/_cd），
#             SecurityUnitData 仅含 dt/open/high/low/close/price/volume/money；
#             停牌用 get_stock_status(query_type='HALT')（_is_halted），涨跌停价用日线 high_limit/low_limit 字段（_limit_prices）
#   - 持仓：get_position(sec) 返回 Position（amount / enable_amount / cost_basis / last_sale_price）
#   - 现金/总资产：context.portfolio.cash / .portfolio_value（PTrade 无 get_cash）
#   - 动态 ETF 池：用 get_market_list()/get_market_detail() 枚举全市场基金，取不到时优雅降级为固定池；
#     全市场 6000+ 标的的成交额查询按 200 只分块（_get_money_avg_series），阈值按实际可交易池估算，避免回测挂起
#   - record()/log.set_level/set_option 等聚宽独有 API 已移除；log 无 warn 方法（_warn 降级 warning/error）
# ============================================================

import numpy as np
import math
import pandas as pd
from datetime import datetime, date, timedelta

import warnings
warnings.filterwarnings("ignore")


# ==================== 日志兼容层（不同 PTrade 版本 LogEngine 方法名不一致） ====================
def _safe_log(msg, *names):
    """按 names 顺序尝试调用 log.<name>(msg)，均不存在则静默丢弃。"""
    for name in names:
        try:
            fn = getattr(log, name)
            fn(msg)
            return
        except AttributeError:
            continue
        except Exception:
            return


def _warn(msg):
    """log.warn 在部分 PTrade 版本不存在，安全降级为 warning/error。"""
    _safe_log(msg, 'warn', 'warning', 'error')


def _debug(msg):
    _safe_log(msg, 'debug')


# ==================== 平台辅助层 ====================
def _pt(code):
    """聚宽代码 → PTrade 代码（XSHG/XSHE → SS/SZ）"""
    return str(code).replace('.XSHG', '.SS').replace('.XSHE', '.SZ')


def _current_dt(context):
    """当前策略时间（PTrade 回测/实盘均为 context.blotter.current_dt）"""
    try:
        return context.blotter.current_dt
    except Exception:
        return datetime.now()


def _today(context):
    return _current_dt(context).date()


def _previous_trading_day():
    """前一交易日（datetime.date）"""
    try:
        return get_trading_day(-1)
    except Exception:
        return date.today()


def _last_n_trade_days(count):
    try:
        days = get_trade_days(end_date=_previous_trading_day(), count=count)
        return list(days)
    except Exception:
        return []


# ==================== 实时行情快照（PTrade 无 get_current_data，改由 handle_data 捕获 data 参数） ====================
_LAST_DATA = {}
_LAST_CTX = None


class _BarUnit(object):
    """把 handle_data 传入的 SecurityUnitData 包装为策略统一访问的行情单元。
    PTrade 的 SecurityUnitData 仅含 dt/open/high/low/close/price/volume/money，
    paused/涨跌停价需通过 get_stock_status/get_history 另行获取（见 _is_halted/_limit_prices）。"""

    def __init__(self, code, raw):
        self._code = code
        self._raw = raw

    def _field(self, *names):
        r = self._raw
        if r is None:
            return None
        for n in names:
            try:
                v = getattr(r, n, None)
                if v is not None:
                    return v
            except Exception:
                pass
        return None

    @property
    def lastPrice(self):
        v = self._field('price', 'close')
        return v if v is not None else 0

    @property
    def price(self):
        return self.lastPrice

    @property
    def close(self):
        return self.lastPrice

    @property
    def volume(self):
        v = self._field('volume')
        return v if v is not None else 0

    @property
    def money(self):
        v = self._field('money')
        return v if v is not None else 0

    @property
    def paused(self):
        return False

    @property
    def highLimit(self):
        return None

    @property
    def lowLimit(self):
        return None

    @property
    def name(self):
        return None

    def get(self, key, default=None):
        return getattr(self, key, default)


def _set_last_data(data, context):
    """由 handle_data / before_trading_start 调用，捕获最新行情快照。"""
    global _LAST_DATA, _LAST_CTX
    _LAST_CTX = context
    if not data:
        return
    out = {}
    try:
        items = data.items() if hasattr(data, 'items') else []
        for code, unit in items:
            out[code] = _BarUnit(code, unit)
    except Exception:
        pass
    if out:
        _LAST_DATA = out


def _cd():
    """返回最近一次捕获的行情快照（dict：code -> _BarUnit），无快照时返回 {}。"""
    return _LAST_DATA or {}


def _cd_field(security, attr, default=None):
    try:
        obj = _cd().get(security)
        if obj is None:
            return default
        return getattr(obj, attr, default)
    except Exception:
        return default


# ==================== 停牌 / 涨跌停价（回测经 get_stock_status / 日线字段获取，按日缓存） ====================
_HALT_CACHE = {}
_LIMIT_CACHE = {}


def _refresh_halt_status(codes, context):
    global _HALT_CACHE
    today = _today(context).strftime('%Y%m%d')
    if today not in _HALT_CACHE:
        result = {}
        codes = list(codes)
        CHUNK = 100
        for i in range(0, len(codes), CHUNK):
            chunk = codes[i:i + CHUNK]
            try:
                res = get_stock_status(chunk, query_type='HALT', query_date=today)
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


def _single_daily_value(code, field, context):
    try:
        df = get_history(1, '1d', field, security_list=code, include=True)
        v = _as_series_values(df)
        if v is not None and len(v) > 0:
            return float(v[-1])
    except Exception:
        pass
    return None


def _current_price(security, context):
    """当前价：优先快照 lastPrice，其次当日最新分钟收盘，最后最近日线收盘。

    与聚宽实时行情口径对齐（新选出的标的可能不在 handle_data 快照内，需按真实
    分钟价回退而非昨收日线，否则买入股数/动量得分偏差）。"""
    obj = _cd().get(security)
    p = (getattr(obj, 'lastPrice', 0) or 0) if obj else 0
    if p:
        return p
    try:
        mdf = get_history(1, '1m', 'close', security_list=security, include=True)
        v = _as_series_values(mdf)
        if v is not None and len(v) > 0:
            val = float(v[-1])
            if val == val:  # not NaN
                return val
    except Exception:
        pass
    return _single_daily_value(security, 'close', context) or 0


def _limit_prices(code, context):
    """当日涨跌停价 (high, low)。回测经日线 high_limit/low_limit 字段获取，失败返回 (None, None) 由调用方跳过限制判断。"""
    today = _today(context).strftime('%Y%m%d')
    key = (today, code)
    if key in _LIMIT_CACHE:
        return _LIMIT_CACHE[key]
    high = _single_daily_value(code, 'high_limit', context)
    low = _single_daily_value(code, 'low_limit', context)
    _LIMIT_CACHE[key] = (high, low)
    return (high, low)


def _get_total_value(context):
    """总资产（context.portfolio.portfolio_value，PTrade 无 get_cash）"""
    try:
        return float(context.portfolio.portfolio_value)
    except Exception:
        pass
    try:
        return float(context.portfolio.cash)
    except Exception:
        return 0.0


def _get_available_cash(context):
    """当前可用现金（context.portfolio.cash）"""
    try:
        return float(context.portfolio.cash)
    except Exception:
        pass
    try:
        return float(context.portfolio.portfolio_value)
    except Exception:
        return 0.0


def _get_position(security):
    try:
        return get_position(security)
    except Exception:
        return None


def _positions_map():
    """返回 {security: Position}，仅含持仓数量>0 的标的。
    PTrade get_positions() 返回 dict {code: Position}（个别版本为 list），此处双兼容。"""
    result = {}
    try:
        ps = get_positions()
        if ps is None:
            return result
        if isinstance(ps, dict):
            items = list(ps.items())
        else:
            items = [(getattr(p, 'sid', None) or getattr(p, 'security', None), p) for p in ps]
        for sec, pos in items:
            if not sec:
                continue
            sec = _pt(sec)
            if _pos_amount(pos) > 0:
                result[sec] = pos
    except Exception:
        pass
    return result


def _pos_amount(pos):
    if pos is None:
        return 0
    return float(getattr(pos, 'amount', None) or getattr(pos, 'quantity', None) or 0)


def _pos_avail(pos):
    if pos is None:
        return 0
    return float(getattr(pos, 'enable_amount', None) or getattr(pos, 'avail_amount', None)
                 or getattr(pos, 'available_amount', None) or 0)


def _pos_cost(pos):
    if pos is None:
        return 0.0
    return float(getattr(pos, 'cost_basis', None) or getattr(pos, 'cost', None)
                 or getattr(pos, 'cost_price', None) or 0.0)


def _pos_price(pos):
    if pos is None:
        return 0.0
    return float(getattr(pos, 'last_sale_price', None) or getattr(pos, 'last_price', None)
                 or getattr(pos, 'price', None) or 0.0)


def _as_series_values(obj):
    """把单只标的的 get_history/get_price 结果规整为一维 numpy 数组（兼容不同版本返回结构）"""
    if obj is None:
        return None
    if isinstance(obj, pd.Series):
        return np.asarray(obj, dtype=float)
    if isinstance(obj, pd.DataFrame):
        for col in ('close', 'volume', 'money'):
            if col in obj.columns:
                return np.asarray(obj[col], dtype=float)
        if obj.shape[1] >= 1:
            return np.asarray(obj.iloc[:, 0], dtype=float)
        return None
    if isinstance(obj, dict):
        for k in ('close', 'volume', 'money'):
            if k in obj:
                return np.asarray(obj[k], dtype=float)
        for k, v in obj.items():
            if isinstance(v, (list, tuple, np.ndarray)):
                return np.asarray(v, dtype=float)
        return None
    return None


def _wide(df, value_col=None):
    """把多标的 get_history 结果规整为宽表（index=时间，columns=代码）。
    PTrade 多标的+单字段返回 DataFrame(columns=代码)；个别版本返回带 code 列的长表，此处兜底 pivot。"""
    if df is None:
        return None
    if isinstance(df, pd.DataFrame) and 'code' in df.columns:
        vcol = value_col
        if vcol is None:
            for c in ('close', 'volume', 'money'):
                if c in df.columns:
                    vcol = c
                    break
        if vcol:
            try:
                return df.pivot_table(index=df.index, columns='code', values=vcol)
            except Exception:
                return df
    return df


def get_security_name(security):
    try:
        if getattr(g, 'etf_names_dict', {}) and security in g.etf_names_dict:
            return g.etf_names_dict[security]
        try:
            d = get_stock_name(security)
            if d and d.get(security):
                return d.get(security)
        except Exception:
            pass
        obj = _cd().get(security)
        if obj is not None:
            n = getattr(obj, 'name', None)
            if n:
                return n
        return security
    except Exception:
        return security


def _get_today_volumes(context, codes):
    """批量取当日累计成交量（分钟线求和，分块避免超大查询挂起）。失败返回 {}。"""
    out = {}
    today = _today(context)
    codes = list(codes)
    CHUNK = 100
    for i in range(0, len(codes), CHUNK):
        chunk = codes[i:i + CHUNK]
        try:
            mdf = get_history(241, '1m', 'volume', security_list=chunk, include=True)
            if mdf is None:
                continue
            if isinstance(mdf, pd.DataFrame) and 'code' in mdf.columns:
                for code, gdf in mdf.groupby('code'):
                    s = gdf['volume']
                    if hasattr(gdf.index, 'date'):
                        s = s[gdf.index.date == today]
                    s = pd.to_numeric(s, errors='coerce').dropna()
                    out[str(code)] = float(s.sum())
            elif isinstance(mdf, pd.DataFrame):
                for code in mdf.columns:
                    s = mdf[code]
                    if hasattr(mdf.index, 'date'):
                        s = s[mdf.index.date == today]
                    s = pd.to_numeric(s, errors='coerce').dropna()
                    out[str(code)] = float(s.sum())
        except Exception:
            continue
    return out


def _get_money_avg_series(codes, count, context, field='money'):
    """分块 get_history 拉取成交额并计算日均，返回 pd.Series(code -> 日均成交额)。
    避免对上千只标的单次 get_history 查询导致回测挂起。
    field='money_corrected'：返回引擎修正后的元成交额（对齐聚宽 get_daily_money_cached
    口径，用于流动性阈值）；真 PTrade 无该字段，get_history 回退 'money'。"""
    result = pd.Series(dtype=float)
    codes = list(codes)
    CHUNK = 200
    for i in range(0, len(codes), CHUNK):
        chunk = codes[i:i + CHUNK]
        try:
            df = _wide(get_history(count, '1d', field, security_list=chunk))
            if df is None or df.empty:
                continue
            df = df.fillna(0.0)
            total = df.sum(axis=0)
            avg = total / count
            for code, v in avg.items():
                if code in chunk:
                    result[str(code)] = float(v)
        except Exception:
            continue
    return result


def _get_money_daily_totals(codes, context):
    """按日汇总样本池成交额，返回 {日期: (总成交额, 有成交只数)}，失败返回 None。"""
    try:
        codes = list(codes)
        CHUNK = 200
        totals = {}
        for i in range(0, len(codes), CHUNK):
            chunk = codes[i:i + CHUNK]
            df = _wide(get_history(3, '1d', 'money', security_list=chunk))
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


def _update_universe(pool=None):
    """刷新 PTrade 股票池（保证 order() 的标的全在 universe 内）"""
    try:
        codes = []
        if pool:
            codes.extend(pool)
        if getattr(g, 'defensive_etf', None) and g.defensive_etf not in codes:
            codes.append(g.defensive_etf)
        set_universe(codes)
    except Exception as e:
        _warn('set_universe 更新失败: %s' % e)


# ==================== 全市场基金枚举（动态池用，尽力实现+优雅降级） ====================
def _get_all_fund_codes():
    """枚举全市场基金代码/名称 {code: name}。
    通过 get_market_list() 遍历所有市场，get_market_detail(mic) 拉取产品，
    按基金代码前缀（SH 5xxxxx / SZ 15xxxx 16xxxx）过滤。失败返回 None（调用方降级）。"""
    try:
        ml = get_market_list()
        if ml is None:
            return None
        rows = []
        if hasattr(ml, 'iterrows'):
            rows = list(ml.iterrows())
        elif isinstance(ml, (list, tuple)):
            rows = [(None, r) for r in ml]
        elif isinstance(ml, dict):
            rows = [(None, ml)]
        fund_codes = {}
        for _, r in rows:
            if r is None:
                continue
            mic = r.get('finance_mic') if hasattr(r, 'get') else None
            if not mic:
                mic = r.get('market_code') or r.get('code') or r.get('market')
            if not mic:
                continue
            try:
                detail = get_market_detail(mic)
            except Exception:
                continue
            if detail is None:
                continue
            cols = list(detail.columns) if hasattr(detail, 'columns') else []
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
                    pn = str(drow[pn_col]) if pn_col else pc
                    fund_codes[pc] = pn
                except Exception:
                    continue
        if not fund_codes:
            return None
        return fund_codes
    except Exception as e:
        _warn('枚举全市场基金失败: %s' % e)
        return None


def _ensure_fund_universe():
    """缓存全市场基金表 g._fund_universe（{code: name}），失败则空表"""
    if getattr(g, '_fund_universe', None) is None:
        fc = _get_all_fund_codes()
        g._fund_universe = fc if fc else {}
    return g._fund_universe


# ==================== 定时任务 ====================
def initialize(context):
    set_benchmark('510300.SS')
    try:
        set_commission(commission_ratio=0.0001, min_commission=5.0, type='ETF')
        set_commission(commission_ratio=0.0001, min_commission=5.0, type='LOF')
    except Exception as e:
        _warn('设置佣金失败(仅回测有效): %s' % e)
    try:
        set_slippage(slippage=0.0002)
    except Exception as e:
        _warn('设置滑点失败(仅回测有效): %s' % e)

    # ==================== ETF池定义 ====================
    # 全球/海外ETF池（含大宗商品和海外市场ETF）
    g.global_etf_pool = [
        # 大宗商品ETF：
        '518880.XSHG',  # (黄金ETF) [ETF]-日均成交额：51.35亿元-上市日期：2013-07-29
        '501018.XSHG',  # (南方原油) [LOF]-日均成交额：24.38亿元-上市日期：2016-06-28
        '161226.XSHE',  # (国投白银LOF) [LOF]-日均成交额：5.44亿元-上市日期：2015-08-17
        '159985.XSHE',  # (豆粕ETF华夏) [ETF]-日均成交额：4.63亿元-上市日期：2019-12-05
        '159980.XSHE',  # (有色ETF大成) [ETF]-日均成交额：3.84亿元-上市日期：2019-12-24
        # 海外ETF：
        '513310.XSHG',  # (中韩芯片) [ETF]-日均成交额：59.37亿元-上市日期：2022-12-22
        '159518.XSHE',  # (标普油气ETF嘉实) [ETF]-日均成交额：27.93亿元-上市日期：2023-11-15
        '159509.XSHE',  # (纳指科技ETF景顺) [ETF]-日均成交额：7.24亿元-上市日期：2023-08-08
        '513100.XSHG',  # (纳指ETF) [ETF]-日均成交额：5.02亿元-上市日期：2013-05-15
        '513520.XSHG',  # (日经ETF) [ETF]-日均成交额：3.72亿元-上市日期：2019-06-25
        '513500.XSHG',  # (标普500) [ETF]-日均成交额：2.89亿元-上市日期：2014-01-15
        '159502.XSHE',  # (标普生物科技ETF嘉实) [ETF]-日均成交额：1.80亿元-上市日期：2024-01-10
        '513400.XSHG',  # (道琼斯) [ETF]-日均成交额：1.70亿元-上市日期：2024-02-02
        '513030.XSHG',  # (德国ETF) [ETF]-日均成交额：0.95亿元-上市日期：2014-09-05
        '513290.XSHG',  # (纳指生物) [ETF]-日均成交额：0.78亿元-上市日期：2022-08-29
        '520830.XSHG',  # (沙特ETF) [ETF]-日均成交额：0.62亿元-上市日期：2024-07-16
        '159529.XSHE',  # (标普消费ETF景顺) [ETF]-日均成交额：0.50亿元-上市日期：2024-02-02
    ]
    g.global_etf_pool = [_pt(c) for c in g.global_etf_pool]
    # 中国ETF池（含港股、指数、行业ETF）
    g.china_etf_pool = [
        # 港股ETF：
        '513090.XSHG',  # (香港证券) [ETF]-日均成交额：54.24亿元-上市日期：2020-03-26
        '513120.XSHG',  # (HK创新药) [ETF]-日均成交额：52.34亿元-上市日期：2022-07-12
        '513180.XSHG',  # (恒指科技) [ETF]-日均成交额：36.66亿元-上市日期：2021-05-25
        '513330.XSHG',  # (恒生互联) [ETF]-日均成交额：20.45亿元-上市日期：2021-02-08
        '513750.XSHG',  # (港股非银) [ETF]-日均成交额：9.55亿元-上市日期：2023-11-27
        '159892.XSHE',  # (恒生医药ETF华夏) [ETF]-日均成交额：7.90亿元-上市日期：2021-10-19
        '513190.XSHG',  # (H股金融) [ETF]-日均成交额：3.74亿元-上市日期：2023-10-11
        '159605.XSHE',  # (中概互联ETF广发) [ETF]-日均成交额：3.19亿元-上市日期：2021-12-02
        '513630.XSHG',  # (香港红利) [ETF]-日均成交额：2.84亿元-上市日期：2023-12-08
        '159323.XSHE',  # (港股通汽车ETF华夏) [ETF]-日均成交额：1.98亿元-上市日期：2025-01-08
        '510900.XSHG',  # (恒生中国) [ETF]-日均成交额：1.46亿元-上市日期：2012-10-22
        '513920.XSHG',  # (央企40) [ETF]-日均成交额：1.38亿元-上市日期：2024-01-05
        '513970.XSHG',  # (恒生消费) [ETF]-日均成交额：0.82亿元-上市日期：2023-04-21
        # 指数ETF：
        '511380.XSHG',  # (转债ETF) [ETF]-日均成交额：115.92亿元-上市日期：2020-04-07
        '512050.XSHG',  # (A500E) [ETF]-日均成交额：48.05亿元-上市日期：2024-11-15
        '510500.XSHG',  # (500ETF) [ETF]-日均成交额：45.45亿元-上市日期：2013-03-15
        '159915.XSHE',  # (创业板ETF易方达) [ETF]-日均成交额：43.55亿元-上市日期：2011-12-09
        '510300.XSHG',  # (300ETF) [ETF]-日均成交额：34.60亿元-上市日期：2012-05-28
        '512100.XSHG',  # (1000ETF) [ETF]-日均成交额：25.26亿元-上市日期：2016-11-04
        '159949.XSHE',  # (创业板50ETF华安) [ETF]-日均成交额：16.52亿元-上市日期：2016-07-22
        '588080.XSHG',  # (科创板50) [ETF]-日均成交额：13.32亿元-上市日期：2020-11-16
        '159967.XSHE',  # (创业板成长ETF华夏) [ETF]-日均成交额：5.29亿元-上市日期：2019-07-15
        '588220.XSHG',  # (科创100F) [ETF]-日均成交额：5.01亿元-上市日期：2023-09-15
        '563300.XSHG',  # (中证2000) [ETF]-日均成交额：4.13亿元-上市日期：2023-09-14
        '510760.XSHG',  # (上证ETF) [ETF]-日均成交额：1.45亿元-上市日期：2020-09-09
        # 行业ETF：
        '588200.XSHG',  # (科创芯片) [ETF]-日均成交额：28.07亿元-上市日期：2022-10-26
        '515880.XSHG',  # (通信ETF) [ETF]-日均成交额：22.39亿元-上市日期：2019-09-06
        '159981.XSHE',  # (能源化工ETF建信) [ETF]-日均成交额：21.63亿元-上市日期：2020-01-17
        '512880.XSHG',  # (证券ETF) [ETF]-日均成交额：16.21亿元-上市日期：2016-08-08
        '513350.XSHG',  # (油气ETF) [ETF]-日均成交额：15.66亿元-上市日期：2023-11-28
        '159326.XSHE',  # (电网设备ETF华夏) [ETF]-日均成交额：14.86亿元-上市日期：2024-09-09
        '159516.XSHE',  # (半导体设备ETF国泰) [ETF]-日均成交额：14.23亿元-上市日期：2023-07-27
        '159206.XSHE',  # (卫星ETF永赢) [ETF]-日均成交额：13.87亿元-上市日期：2025-03-14
        '512480.XSHG',  # (半导体) [ETF]-日均成交额：13.07亿元-上市日期：2019-06-12
        '159363.XSHE',  # (创业板人工智能ETF华宝) [ETF]-日均成交额：10.50亿元-上市日期：2024-12-16
        '159870.XSHE',  # (化工ETF鹏华) [ETF]-日均成交额：10.03亿元-上市日期：2021-03-03
        '512400.XSHG',  # (有色ETF) [ETF]-日均成交额：9.97亿元-上市日期：2017-09-01
        '159755.XSHE',  # (电池ETF广发) [ETF]-日均成交额：8.58亿元-上市日期：2021-06-24
        '588170.XSHG',  # (科创半导) [ETF]-日均成交额：7.74亿元-上市日期：2025-04-08
        '159992.XSHE',  # (创新药ETF银华) [ETF]-日均成交额：7.59亿元-上市日期：2020-04-10
        '159995.XSHE',  # (芯片ETF华夏) [ETF]-日均成交额：7.51亿元-上市日期：2020-02-10
        '512890.XSHG',  # (红利低波) [ETF]-日均成交额：6.79亿元-上市日期：2019-01-18
        '515220.XSHG',  # (煤炭ETF) [ETF]-日均成交额：6.44亿元-上市日期：2020-03-02
        '159566.XSHE',  # (储能电池ETF易方达) [ETF]-日均成交额：6.31亿元-上市日期：2024-02-08
        '159819.XSHE',  # (人工智能ETF易方达) [ETF]-日均成交额：6.26亿元-上市日期：2020-09-23
        '512800.XSHG',  # (银行ETF) [ETF]-日均成交额：6.13亿元-上市日期：2017-08-03
        '512690.XSHG',  # (酒ETF) [ETF]-日均成交额：5.99亿元-上市日期：2019-05-06
        '515050.XSHG',  # (5GETF) [ETF]-日均成交额：5.93亿元-上市日期：2019-10-16
        '562500.XSHG',  # (机器人) [ETF]-日均成交额：5.83亿元-上市日期：2021-12-29
        '512170.XSHG',  # (医疗ETF) [ETF]-日均成交额：5.63亿元-上市日期：2019-06-17
        '517520.XSHG',  # (黄金股) [ETF]-日均成交额：5.01亿元-上市日期：2023-11-01
        '159869.XSHE',  # (游戏ETF华夏) [ETF]-日均成交额：4.77亿元-上市日期：2021-03-05
        '512070.XSHG',  # (证券保险) [ETF]-日均成交额：4.61亿元-上市日期：2014-07-18
        '159611.XSHE',  # (电力ETF广发) [ETF]-日均成交额：4.42亿元-上市日期：2022-01-07
        '562800.XSHG',  # (稀有金属) [ETF]-日均成交额：4.39亿元-上市日期：2021-09-27
        '515120.XSHG',  # (创新药) [ETF]-日均成交额：4.34亿元-上市日期：2021-01-04
        '512010.XSHG',  # (医药ETF) [ETF]-日均成交额：4.27亿元-上市日期：2013-10-28
        '510880.XSHG',  # (红利ETF) [ETF]-日均成交额：3.97亿元-上市日期：2007-01-18
        '515790.XSHG',  # (光伏ETF) [ETF]-日均成交额：3.87亿元-上市日期：2020-12-18
        '515980.XSHG',  # (人工智能) [ETF]-日均成交额：3.78亿元-上市日期：2020-02-10
        '512660.XSHG',  # (军工ETF) [ETF]-日均成交额：3.75亿元-上市日期：2016-08-08
        '159928.XSHE',  # (消费ETF汇添富) [ETF]-日均成交额：3.66亿元-上市日期：2013-09-16
        '512710.XSHG',  # (军工龙头) [ETF]-日均成交额：3.60亿元-上市日期：2019-08-26
        '560860.XSHG',  # (工业有色) [ETF]-日均成交额：3.57亿元-上市日期：2023-03-13
        '515030.XSHG',  # (新汽车) [ETF]-日均成交额：3.33亿元-上市日期：2020-03-04
        '159766.XSHE',  # (旅游ETF富国) [ETF]-日均成交额：3.30亿元-上市日期：2021-07-23
        '159218.XSHE',  # (卫星ETF招商) [ETF]-日均成交额：3.21亿元-上市日期：2025-05-22
        '159852.XSHE',  # (软件ETF嘉实) [ETF]-日均成交额：3.19亿元-上市日期：2021-02-09
        '516160.XSHG',  # (新能源) [ETF]-日均成交额：3.07亿元-上市日期：2021-02-04
        '516150.XSHG',  # (稀土基金) [ETF]-日均成交额：3.03亿元-上市日期：2021-03-17
        '159227.XSHE',  # (航空航天ETF华夏) [ETF]-日均成交额：2.98亿元-上市日期：2025-05-16
        '159583.XSHE',  # (通信ETF富国) [ETF]-日均成交额：2.93亿元-上市日期：2024-07-08
        '588790.XSHG',  # (科创智能) [ETF]-日均成交额：2.62亿元-上市日期：2025-01-09
        '159865.XSHE',  # (养殖ETF国泰) [ETF]-日均成交额：2.44亿元-上市日期：2021-03-08
        '512980.XSHG',  # (传媒ETF) [ETF]-日均成交额：2.43亿元-上市日期：2018-01-19
        '159851.XSHE',  # (金融科技ETF华宝) [ETF]-日均成交额：2.27亿元-上市日期：2021-03-19
        '561360.XSHG',  # (石油ETF) [ETF]-日均成交额：2.04亿元-上市日期：2023-10-31
        '561980.XSHG',  # (芯片设备) [ETF]-日均成交额：2.01亿元-上市日期：2023-09-01
        '562590.XSHG',  # (半导材料) [ETF]-日均成交额：1.76亿元-上市日期：2023-10-18
        '512200.XSHG',  # (地产ETF) [ETF]-日均成交额：1.71亿元-上市日期：2017-09-25
        '159732.XSHE',  # (消费电子ETF华夏) [ETF]-日均成交额：1.62亿元-上市日期：2021-08-23
        '159667.XSHE',  # (工业母机ETF国泰) [ETF]-日均成交额：1.58亿元-上市日期：2022-10-26
        '516510.XSHG',  # (云计算) [ETF]-日均成交额：1.49亿元-上市日期：2021-04-07
        '159840.XSHE',  # (锂电池ETF工银) [ETF]-日均成交额：1.42亿元-上市日期：2021-08-20
        '159998.XSHE',  # (计算机ETF天弘) [ETF]-日均成交额：1.30亿元-上市日期：2020-04-13
        '159825.XSHE',  # (农业ETF富国) [ETF]-日均成交额：1.15亿元-上市日期：2020-12-29
        '512670.XSHG',  # (国防ETF) [ETF]-日均成交额：1.12亿元-上市日期：2019-08-01
        '159883.XSHE',  # (医疗器械ETF永赢) [ETF]-日均成交额：1.05亿元-上市日期：2021-04-30
        '515210.XSHG',  # (钢铁ETF) [ETF]-日均成交额：1.01亿元-上市日期：2020-03-02
        '515400.XSHG',  # (大数据) [ETF]-日均成交额：0.94亿元-上市日期：2021-01-20
        '159256.XSHE',  # (创业板软件ETF华夏) [ETF]-日均成交额：0.83亿元-上市日期：2025-08-04
        '561330.XSHG',  # (矿业ETF) [ETF]-日均成交额：0.83亿元-上市日期：2022-11-01
        '515170.XSHG',  # (食品饮料) [ETF]-日均成交额：0.67亿元-上市日期：2021-01-13
        '159638.XSHE',  # (高端装备ETF嘉实) [ETF]-日均成交额：0.56亿元-上市日期：2022-08-12
        '516520.XSHG',  # (智能驾驶) [ETF]-日均成交额：0.47亿元-上市日期：2021-03-01
        '513360.XSHG',  # (教育ETF) [ETF]-日均成交额：0.43亿元-上市日期：2021-06-17
        '516190.XSHG',  # (文娱ETF) [ETF]-日均成交额：0.18亿元-上市日期：2021-09-17
    ]
    g.china_etf_pool = [_pt(c) for c in g.china_etf_pool]
    # 固定ETF池 = 全球池 + 中国池（正常期使用）
    g.fixed_etf_pool = g.global_etf_pool + g.china_etf_pool

    g.avg_etf_money_threshold = None
    g.filtered_fixed_pool = []
    g.dynamic_etf_pool = []
    g.merged_etf_pool = []
    g.ranked_etfs_result = []
    g.filtered_global_pool = []
    g.global_threshold_divisor = 20000  # 全市场ETF流动性阈值除数，资金大建议改为3000避免买到盘子小的etf

    g.is_a_share_weak = False
    g.weak_period_ma_lookback = 10
    g.weak_start_date = None
    g.weak_days_count = 0
    g.max_weak_days = 20

    g.holdings_num = 2
    g.cross_slot1_floor = 0.3          # slot1 另一资产大类动量下限
    g.cross_slot1_retain_ratio = 0.85  # slot1 保留粘性：现有同类持仓 ≥ 类首×该值保留
    g.cross_adaptive = True            # 自适应权重：强腿多得
    g.cross_weight_cap = 0.85          # slot0 权重上限
    g.target_weights = [0.5, 0.5]      # 默认双持仓等权（select_cross_asset_dual 会覆盖）
    g.defensive_etf = _pt("511880.XSHG")  # 银华日利 货币ETF
    g.min_money = 10
    g.target_etfs_list = []
    g.etf_names_dict = {}
    g.cache_date = None
    g.yesterday_close_cache = {}

    g.lookback_days = 25
    g.min_score_threshold = 0
    g.max_score_threshold = 5
    g.score_threshold_ratio = 0.9

    g.enable_r2_filter = True
    g.r2_threshold = 0.4
    g.enable_ma_filter = True
    g.ma_lookback = 10
    g.ma_threshold = 1.0
    g.enable_volume_check = True
    g.volume_lookback = 5
    g.volume_threshold = 1.8
    g.enable_loss_filter = True
    g.loss = 0.97

    g.max_portfolio_value = 0
    g.drawdown_threshold = 0.03
    g.drawdown_records = []

    g.use_fixed_stop_loss = True
    g.fixedStopLossThreshold = 0.95

    # ==================== v5.3 改进开关 ====================
    g.enable_profit_protect = True                       # A1 盈利保护止损：曾浮盈≥阈值后止损上移至成本价
    g.profit_protect_trigger = 0.05
    g.profit_protect_stop = 1.04                         # A1 保护线：1.0=保本，>1.0=锁盈（v5.4 D1，最终配置 1.04）
    g._profit_protected = {}                             # A1 状态：code -> 是否已触发盈利保护
    g.hold_buffer = 1.0                                  # A2 持仓宽容：回测验证为负贡献，默认关闭（1.0=关闭）
    g.weak_exit_ma_lookback = 15                         # A3 走弱期退出均线周期（进入仍用 weak_period_ma_lookback）

    # ==================== v5.4 胜率导向开关 ====================
    # D2a 动量下限=D2b R²阈值/D2c 量比上限/D2d 单日跌幅上限 复用上方原过滤参数（min_score_threshold/r2_threshold/volume_threshold/loss）
    g.enable_take_profit = False                         # D3 高位回落止盈
    g.take_profit_ratio = 0.08                           # D3 触发：曾浮盈≥8%
    g.take_profit_pullback = 0.03                        # D3 回落：从持仓峰值回落≥3% 卖出
    g._peak_price = {}                                   # D3 状态：code -> 持仓期间最高价

    # ==================== 定时任务（PTrade 版本） ====================
    # 晨间流水线：PTrade 用 before_trading_start 触发（回测/实盘都可靠，避免 09:00 在分钟回测不触发）
    # run_daily(morning_routine, time='09:00')  -- 已改为 before_trading_start
    run_daily(context, check_weak_period_daily, time='09:40')    # 09:40 走弱期判断+池子更新
    run_daily(context, afternoon_routine, time='13:10')          # 动量计算与排序（需早于卖出时间）
    run_daily(context, sell_routine, time='13:10')               # 卖出流水线（需早于买入时间）
    run_daily(context, buy_routine, time='13:10')                # 买入流水线
    # 收盘重置：改为 after_trading_end
    # 分钟级固定止损：在 handle_data 中执行

    # 初始化股票池（PTrade 要求交易标的存在于 universe 内）
    _update_universe(g.fixed_etf_pool)

    log.info("【五福闹新春】v5.4（双持仓自适应版）(PTrade 移植版)！")

    log.info("""
【策略参数初始化完成】
=== ETF池配置 ===
- 全球/海外ETF池: %d只
- 国内ETF池: %d只
- 固定池合计: %d只
=== 大A走弱期判定 ===
- MA均线周期: %d日
- 进入条件: 至少3/4指数低于MA%d
- 退出条件: 至少3/4指数站上MA%d
- 最长持续: %d个交易日
=== 动量得分过滤 ===
- 周期: %d天
- 得分阈值: [%d, %d]
- 调仓系数: %.1f
=== 过滤条件 ===
- 正常期 R²过滤: %s (阈值>%.1f)
- 走弱期 均线过滤: %s (MA%d×%.1f)
- 通用 成交量过滤: %s (近%d日均量比<%.1f)
- 通用 短期风控: %s (近3日单日跌幅<%.0f%%)
=== 止损机制 ===
- 分钟级固定比例止损: %s (成本价×%.0f%%)
- A1 盈利保护止损: %s (浮盈≥%.0f%%后止损上移至成本×%.2f)
=== v5.3 其他改进 ===
- A2 持仓宽容: %s (持仓得分≥候选池门槛×%.2f时保留，回测验证为负贡献)
- A3 走弱期退出确认: %s (退出需3/4指数站上MA%d)
=== v5.4 胜率改进 ===
- D2a 动量下限: %d  D2b R²阈值: %.2f  D2c 量比上限: %.2f  D2d 单日跌幅上限: %.0f%%
- D3 高位回落止盈: %s (曾浮盈≥%.0f%%后从峰值回落≥%.0f%%卖出)
=== 其他配置 ===
- 持仓数量: %d只
- 双持仓: slot1下限: %.2f 保留粘性: %.2f 自适应: %s 权重上限: %.2f
- 防御ETF: %s
- 最小交易额: %d元
- 基准: 510300.SS
""" % (
        len(g.global_etf_pool), len(g.china_etf_pool), len(g.fixed_etf_pool),
        g.weak_period_ma_lookback, g.weak_period_ma_lookback, g.weak_exit_ma_lookback,
        g.max_weak_days,
        g.lookback_days, g.min_score_threshold, g.max_score_threshold,
        g.score_threshold_ratio,
        '启用' if g.enable_r2_filter else '禁用', g.r2_threshold,
        '启用' if g.enable_ma_filter else '禁用', g.ma_lookback, g.ma_threshold,
        '启用' if g.enable_volume_check else '禁用', g.volume_lookback, g.volume_threshold,
        '启用' if g.enable_loss_filter else '禁用', (1 - g.loss) * 100,
        '启用' if g.use_fixed_stop_loss else '禁用', g.fixedStopLossThreshold * 100,
        '启用' if g.enable_profit_protect else '禁用', g.profit_protect_trigger * 100, g.profit_protect_stop,
        '启用' if g.hold_buffer < 1.0 else '禁用', g.hold_buffer,
        '启用' if g.weak_exit_ma_lookback != g.weak_period_ma_lookback else '禁用', g.weak_exit_ma_lookback,
        g.min_score_threshold, g.r2_threshold, g.volume_threshold, (1 - g.loss) * 100,
        '启用' if g.enable_take_profit else '禁用', g.take_profit_ratio * 100, g.take_profit_pullback * 100,
        g.holdings_num, g.cross_slot1_floor, g.cross_slot1_retain_ratio,
        '启用' if g.cross_adaptive else '禁用', g.cross_weight_cap,
        g.defensive_etf, g.min_money,
    ))


def before_trading_start(context, data):
    """PTrade 晨间钩子：替代聚宽 09:00 定时任务"""
    _set_last_data(data, context)
    morning_routine(context)


def after_trading_end(context):
    """PTrade 收盘钩子：替代聚宽 15:10 定时任务"""
    reset_daily_flags(context)


def handle_data(context, data):
    """盘中每分钟调用（策略回测/实盘频率需设为分钟级）：分钟级固定止损"""
    _set_last_data(data, context)
    minute_level_stop_loss(context)


def check_weak_period_daily(context):
    check_a_share_weak_period(context)
    midday_routine(context)


def morning_routine(context):
    log.info("★" * 80)
    log.info("▶️ 【晨间流水线】启动...")
    log.info("【持仓检查】检查当前持仓状态...")
    check_positions(context)
    log.info("【回撤监控】监控策略回撤...")
    monitor_drawdown(context)
    log.info("【流动性阈值】计算全市场ETF流动性阈值...")
    calculate_global_etf_threshold(context)
    log.info("⏸️ 【晨间流水线】执行完毕！")


def midday_routine(context):
    log.info("★" * 80)
    log.info("▶️ 【早盘流水线】启动...")
    if g.is_a_share_weak:
        log.info("🔴 【走弱期池更新】仅对全球/海外ETF池进行流动性过滤...")
        filter_global_pool_by_volume(context)
        log.info("【走弱期池更新完成】过滤后全球池: %d只" % len(g.filtered_global_pool))
    else:
        log.info("🟢 【正常期池更新】执行动态池更新、固定池过滤、合并池...")
        log.info("【动态池更新】更新行业ETF动态池（各行业流动性最佳ETF）...")
        update_sector_pool(context)
        log.info("【固定池过滤】过滤固定ETF池流动性...")
        filter_fixed_pool_by_volume(context)
        log.info("【合并池】合并固定池与动态池...")
        daily_merge_etf_pools(context)
        log.info("【正常期池更新完成】合并池: %d只" % len(g.merged_etf_pool))
    log.info("⏸️ 【早盘流水线】执行完毕！")


def afternoon_routine(context):
    log.info("★" * 80)
    log.info("▶️ 【午盘流水线】启动...")
    if g.is_a_share_weak:
        if hasattr(g, 'filtered_global_pool') and g.filtered_global_pool:
            g.merged_etf_pool = list(set(g.filtered_global_pool))
        else:
            g.merged_etf_pool = list(set(g.global_etf_pool))
        g.merged_etf_pool.sort()
        log.info("🔴 【大A走弱期】使用过滤后全球/海外ETF池，共%d只" % len(g.merged_etf_pool))
    else:
        log.info("🟢 【大A正常期】使用合并池，共%d只" % len(g.merged_etf_pool))
    _update_universe(g.merged_etf_pool)
    log.info("【动量计算】计算ETF动量得分与排序...")
    calculate_and_log_ranked_etfs(context)
    log.info("⏸️ 【午盘流水线】执行完毕！")


def sell_routine(context):
    log.info("★" * 80)
    log.info("▶️ 【卖出流水线】启动...")
    execute_sell_trades(context)
    log.info("⏸️ 【卖出流水线】执行完毕！")


def buy_routine(context):
    log.info("★" * 80)
    log.info("▶️ 【买入流水线】启动...")
    execute_buy_trades(context)
    log.info("⏸️ 【买入流水线】执行完毕！")


def reset_daily_flags(context):
    g.cache_date = None
    g.yesterday_close_cache = {}
    log.info("🔄 收盘缓存重置完成")


def check_positions(context):
    try:
        current_data = _cd()
        for security, position in _positions_map().items():
            security_name = get_security_name(security)
            log.info("📊 【持仓检查】%s %s, 数量: %d, 成本: %.3f, 当前价: %.3f" % (
                security, security_name,
                int(_pos_amount(position)), _pos_cost(position), _pos_price(position)))
            if _is_halted(security, context):
                log.info("⚠️ %s %s 今日停牌" % (security, security_name))
    except Exception as e:
        _warn("【持仓检查】执行异常: %s" % e)


def monitor_drawdown(context):
    try:
        current_value = _get_total_value(context)
        if current_value > g.max_portfolio_value:
            g.max_portfolio_value = current_value
        if g.max_portfolio_value > 0:
            current_drawdown = (g.max_portfolio_value - current_value) / g.max_portfolio_value
            if current_drawdown >= g.drawdown_threshold:
                record = {
                    'date': _today(context).strftime('%Y-%m-%d'),
                    'drawdown': current_drawdown,
                    'portfolio_value': current_value,
                    'max_value': g.max_portfolio_value,
                    'is_weak': g.is_a_share_weak
                }
                positions_info = []
                for security, position in _positions_map().items():
                    security_name = get_security_name(security)
                    positions_info.append("%s:%d股" % (security_name, int(_pos_amount(position))))
                record['positions'] = positions_info
                g.drawdown_records.append(record)
                log.info("【回撤预警】回撤达到 %.2f%% (阈值: %.0f%%)" % (current_drawdown * 100, g.drawdown_threshold * 100))
                log.info("  当前净值: %s  |  最高净值: %s" % (format(current_value, ',.0f'), format(g.max_portfolio_value, ',.0f')))
                log.info("  大A状态: %s" % ('走弱期' if g.is_a_share_weak else '正常期'))
                log.info("  持仓: %s" % (', '.join(positions_info) if positions_info else '空仓'))
    except Exception as e:
        log.error("【回撤监控】计算异常: %s" % e)


def calculate_global_etf_threshold(context):
    log.info("【全局阈值更新】开始计算全市场ETF流动性门槛")
    try:
        # 缓存全市场 ETF 列表（仅首次获取，PTrade 用 get_market_detail 枚举）
        if not hasattr(g, '_cached_etf_universe') or g._cached_etf_universe is None:
            fund_map = _ensure_fund_universe()
            g._cached_etf_universe = list(fund_map.keys()) if fund_map else []
            log.info("全市场基金总数: %d只 (已缓存)" % len(g._cached_etf_universe))
        # 阈值基于全市场基金（与聚宽口径一致：全市场总成交额 / 除数）。
        etf_list = list(g._cached_etf_universe)
        if not etf_list:
            _warn("未找到任何场内ETF，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        trade_days = _last_n_trade_days(3)
        if len(trade_days) < 3:
            _warn("仅有%d个有效交易日，使用保守阈值1000万" % len(trade_days))
            g.avg_etf_money_threshold = 10000000
            return
        avg_daily_money = _get_money_avg_series(etf_list, 3, context, field='money_corrected')
        if avg_daily_money.empty:
            _warn("无成交额数据，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        # 分日汇总用于日志展示
        daily_totals = _get_money_daily_totals(etf_list, context)
        if daily_totals is not None:
            for day, (money, count) in daily_totals.items():
                log.info("  %s 样本池ETF总成交额: %.2f亿元 (%d只ETF有成交)" % (day, money / 1e8, count))
        avg_total_money = avg_daily_money.sum()
        threshold = avg_total_money / g.global_threshold_divisor
        g.avg_etf_money_threshold = threshold
        log.info("【全局阈值更新完成】近3日样本池日均总成交额=%.2f亿元，阈值=%.0f万元(%s元)" % (
            avg_total_money / 1e8, threshold / 1e4, format(threshold, ',.0f')))
    except Exception as e:
        _warn("计算全局阈值异常: %s，使用保守阈值1000万" % e)
        g.avg_etf_money_threshold = 10000000


def filter_global_pool_by_volume(context):
    log.info("【全球池过滤】开始执行")
    if getattr(g, 'avg_etf_money_threshold', None) is None:
        log.info("【全球池过滤】阈值未初始化，立即计算")
        calculate_global_etf_threshold(context)
    if not g.global_etf_pool:
        log.info("【全球池过滤】全球池为空，跳过过滤")
        g.filtered_global_pool = []
        return
    dynamic_threshold = g.avg_etf_money_threshold
    log.info("【全球池过滤】使用流动性门槛=日均%.0f万元" % (dynamic_threshold / 1e4))
    TRADE_DAYS_COUNT = 3
    try:
        avg_daily_money = _get_money_avg_series(g.global_etf_pool, TRADE_DAYS_COUNT, context)
        if avg_daily_money.empty:
            _warn("【全球池过滤】无成交额数据，使用原始全球池")
            g.filtered_global_pool = g.global_etf_pool[:]
            return
        qualified = avg_daily_money[avg_daily_money > dynamic_threshold]
        new_global_pool = qualified.index.tolist()
        removed = set(g.global_etf_pool) - set(new_global_pool)
        if removed:
            removed_info = []
            for code in removed:
                try:
                    name = getattr(g, 'etf_names_dict', {}).get(code, str(code))
                    money = avg_daily_money.get(code, 0)
                    removed_info.append("%s(%s) %.2f亿" % (name, code, money / 1e8))
                except Exception:
                    removed_info.append(code)
            log.info("【全球池过滤】剔除低流动性ETF(%d只)" % len(removed))
        g.filtered_global_pool = new_global_pool
        log.info("【全球池过滤】保留高流动性ETF(%d只)" % len(new_global_pool))
    except Exception as e:
        _warn("【全球池过滤】异常: %s" % e)
        g.filtered_global_pool = g.global_etf_pool[:]


def update_sector_pool(context):
    log.info("【动态池更新】开始执行")
    if g.avg_etf_money_threshold is None:
        log.info("【动态池更新】阈值未初始化，立即计算")
        calculate_global_etf_threshold(context)

    FUND_COMPANIES = sorted(list(set([
        '易方达', '广发', '华夏', '华安', '嘉实', '富国', '招商', '鹏华', '南方', '汇添富', '国泰', '平安',
        '银华', '天弘', '建信', '工银', '华泰柏瑞', '博时', '景顺长城', '景顺', '华宝', '申万菱信', '万家', '中欧',
        '兴证全球', '浙商', '诺安', '前海开源', '泰康', '泰达宏利', '农银汇理', '交银', '东方红', '财通', '华商',
        '国联', '永赢', '金鹰', '德邦', '创金合信', '西部利得', '圆信永丰', '泓德', '汇安', '诺德', '恒生前海',
        '华润元大', '大成', '海富通', '摩根', '华泰', '中信', '中银', '兴全', '国信', '长城', '中金', '浙商证券',
        '东海', '东吴', '浦银安盛', '信达澳亚', '中加', '中航', '中融', '中邮', '中庚', '中信保诚', '中信建投',
        '中银国际', '中银证券', '九泰', '交银施罗德', '光大保德信', '兴银', '农银', '国投瑞银', '国海富兰克林',
        '国联安', '国金', '太平', '方正富邦', '民生加银', '汇丰晋信', '银河', '长信', '长安', '长盛', '长江证券', '鹏扬'
    ])), key=len, reverse=True)

    NOISE_WORDS = sorted(list(set([
        '6666', '8888', '9999', 'A类', 'AH', 'B', 'BS', 'C', 'C类', 'CS', 'DB', 'E', 'E类',
        'ETF', 'ETF基金', 'ETF联接', 'FG', 'G60', 'GF', 'GT', 'HGS', 'LOF', 'LOF基金', 'LOF联接',
        'SG', 'SZ', 'TF', 'TK', 'WJ', 'YH', 'ZS', 'ZZ', '板块', '策略', '产业', '场内', '场外', '低波',
        '基本面', '基金', '精选', '联接', '联接基金', '量化', '龙头', '民企', '民营', '国企', '央企', '智能',
        '全指', '上市开放式', '指基', '指增', '指数', '指数A', '指数C', '指数ETF', '指数基金', '主题', '增强',
        '上海', '黄', '30', '50', '100', '300', '500', '1000', '2000', '大', '新', '四川', '浙江', '湖北',
    ])), key=len, reverse=True)

    SPECIAL_GROUPS = sorted([
        {'name': '香港组', 'keywords': sorted(['恒生', '恒指', '港股', '港股通', 'H股', '香港', '港', 'HKC', 'HK', 'HGS', 'H', '中概', 'HS科技'], key=len, reverse=True),
         'remove_words': sorted(['恒生', '恒指', '港股', '港股通', 'H股', '香港', '港', 'HKC', 'HK', 'HGS', 'H', '中概', 'HS'], key=len, reverse=True)},
        {'name': '科创组', 'keywords': sorted(['科创', '科创板', '科综', 'KC', 'K C', '双创', '科创创业', '创创'], key=len, reverse=True),
         'remove_words': sorted(['科创', '科创板', '科综', 'KC', 'K C', '双创', '科创创业', '创创', '债券', '债汇', '债指', '债沪', '债易', '债基', '债兴', '债摩', '债', 'AAA'], key=len, reverse=True)},
        {'name': '创业组', 'keywords': sorted(['创业板', '创业', '创板', '创成长'], key=len, reverse=True),
         'remove_words': sorted(['创业板', '创业', '创板', '创成长'], key=len, reverse=True)},
        {'name': '美指组', 'keywords': sorted(['标普', '纳指', '纳斯达克'], key=len, reverse=True),
         'remove_words': sorted(['标普', '纳指', '纳斯达克'], key=len, reverse=True)}
    ], key=lambda x: max(len(kw) for kw in x['keywords']), reverse=True)

    exclude_keywords = sorted(list(set([
        '300', '500', '1000', '2000', '800', '30', '50', '100', '180', '200',
        '沪深', '中证', '上证', '深证', '深成', 'A50', 'A100', 'A500', '深100',
        '短融', '可转债', '转债', '双债', '利率债', '国债', '地债', '政金债', '国开债', '基准国债', '新综债',
        '信用债', '企业债', '公司债', '城投债', '城投', '美元债', '沪公司债', '科创债', '科债', '科创AAA',
        '自由现金流', '现金流', '现金流E', '现金流基', '现金流TF', '现金流全', '300现金流', '800现金流',
        '货币', '现金', '快线', '快钱', '中银现金', '500现金', '800现金', '现金800', '现金自由', '现金指数',
        '全指现金', '现金全指', 'ESG', 'MSCI', 'MS', '债',
    ])), key=len, reverse=True)

    try:
        fund_map = _ensure_fund_universe()
        if not fund_map:
            _warn("【动态池更新】无法枚举全市场基金，跳过动态池（降级为固定池）")
            g.dynamic_etf_pool = []
            return
        g.etf_names_dict = dict(fund_map)
        etf_list = list(fund_map.keys())
    except Exception as e:
        _warn("获取全市场ETF列表失败: %s" % e)
        g.dynamic_etf_pool = []
        return

    log.info("【动态池更新】全市场基金总数: %d只" % len(etf_list))
    normal_etfs = []
    special_etfs = []
    special_group_map = {}
    excluded_count = 0

    for code in etf_list:
        try:
            name = g.etf_names_dict.get(code, str(code))
            is_special = False
            matched_group = None
            for group in SPECIAL_GROUPS:
                for kw in group['keywords']:
                    if kw in name:
                        is_special = True
                        matched_group = group['name']
                        break
                if is_special:
                    break
            is_excluded = False
            for k in exclude_keywords:
                if k in name:
                    is_excluded = True
                    excluded_count += 1
                    break
            if not is_excluded:
                if is_special:
                    special_etfs.append(code)
                    special_group_map[code] = matched_group
                else:
                    normal_etfs.append(code)
        except Exception:
            continue

    group_counts = {}
    for code in special_etfs:
        group_name = special_group_map.get(code, '未知')
        group_counts[group_name] = group_counts.get(group_name, 0) + 1
    log.info("【动态池更新】特别组分布: %s" % group_counts)
    log.info("【动态池更新】进入特别组: %d只" % len(special_etfs))
    log.info("【动态池更新】进入普通组: %d只" % len(normal_etfs))
    log.info("【动态池更新】排除ETF: %d只" % excluded_count)

    TRADE_DAYS_COUNT = 3
    dynamic_threshold = g.avg_etf_money_threshold

    def filter_by_liquidity(etf_codes, group_name):
        if not etf_codes:
            return pd.Series(dtype=float), 0
        try:
            avg_daily_money = _get_money_avg_series(etf_codes, TRADE_DAYS_COUNT, context)
            if avg_daily_money.empty:
                return pd.Series(dtype=float), len(etf_codes)
            qualified_series = avg_daily_money[avg_daily_money > dynamic_threshold].sort_values(ascending=False)
            filtered_out = len(etf_codes) - len(qualified_series)
            return qualified_series, filtered_out
        except Exception:
            return pd.Series(dtype=float), len(etf_codes)

    normal_qualified, normal_filtered_out = filter_by_liquidity(normal_etfs, "普通组")
    special_qualified, special_filtered_out = filter_by_liquidity(special_etfs, "特别组")
    normal_sorted = normal_qualified.index.tolist()
    special_sorted = special_qualified.index.tolist()
    log.info("【动态池更新】特别组流动性过滤: %d→%d只" % (len(special_etfs), len(special_sorted)))
    log.info("【动态池更新】普通组流动性过滤: %d→%d只" % (len(normal_etfs), len(normal_sorted)))

    if not normal_sorted and not special_sorted:
        _warn("【动态池更新】无ETF通过流动性过滤")
        g.dynamic_etf_pool = []
        return

    def get_remove_words_for_etf(_, is_special, matched_group_name):
        if not is_special:
            return []
        for group in SPECIAL_GROUPS:
            if group['name'] == matched_group_name:
                return group['remove_words']
        return []

    def clean_name(original_name, is_special=False, matched_group_name=None):
        cleaned = original_name
        for company in FUND_COMPANIES:
            cleaned = cleaned.replace(company, '')
        if is_special and matched_group_name:
            for word in get_remove_words_for_etf(original_name, is_special, matched_group_name):
                cleaned = cleaned.replace(word, '')
        for noise in NOISE_WORDS:
            cleaned = cleaned.replace(noise, '')
        return cleaned.strip()

    normal_industry_groups = {}
    for code in normal_sorted:
        try:
            original_name = g.etf_names_dict.get(code, str(code))
            money = normal_qualified[code]
            cleaned = clean_name(original_name, is_special=False)
            if cleaned == '':
                continue
            industry_key = cleaned[:2] if len(cleaned) >= 2 else cleaned
            if industry_key not in normal_industry_groups:
                normal_industry_groups[industry_key] = []
            normal_industry_groups[industry_key].append({
                'code': code, 'original_name': original_name, 'cleaned_name': cleaned,
                'money': money, 'group_type': '普通'
            })
        except Exception:
            continue

    special_industry_groups = {}
    for code in special_sorted:
        try:
            original_name = g.etf_names_dict.get(code, str(code))
            matched_group = special_group_map.get(code, '未知')
            money = special_qualified[code]
            cleaned = clean_name(original_name, is_special=True, matched_group_name=matched_group)
            if cleaned == '':
                continue
            industry_key = cleaned[:2] if len(cleaned) >= 2 else cleaned
            group_key = "%s_%s" % (matched_group, industry_key)
            if group_key not in special_industry_groups:
                special_industry_groups[group_key] = []
            special_industry_groups[group_key].append({
                'code': code, 'original_name': original_name, 'cleaned_name': cleaned,
                'money': money, 'group_type': matched_group, 'display_group': matched_group
            })
        except Exception:
            continue

    final_pool_info = []
    for industry_key, items in normal_industry_groups.items():
        sorted_items = sorted(items, key=lambda x: x['money'], reverse=True)
        final_pool_info.append(sorted_items[0])
    for group_key, items in special_industry_groups.items():
        sorted_items = sorted(items, key=lambda x: x['money'], reverse=True)
        final_pool_info.append(sorted_items[0])

    final_pool_info_sorted = sorted(final_pool_info, key=lambda x: x['money'], reverse=True)
    top_300 = final_pool_info_sorted[:300]
    g.dynamic_etf_pool = [item['code'] for item in top_300]
    log.info("【动态池更新完成】动态池共%d只ETF" % len(g.dynamic_etf_pool))
    if len(g.dynamic_etf_pool) <= 10:
        for item in top_300[:10]:
            log.info("  %s %s 日均成交额: %.2f亿" % (item['code'], item['original_name'], item['money'] / 1e8))


def filter_fixed_pool_by_volume(context):
    log.info("【固定池过滤】开始执行")
    if getattr(g, 'avg_etf_money_threshold', None) is None:
        log.info("【固定池过滤】阈值未初始化，立即计算")
        calculate_global_etf_threshold(context)
    if not g.fixed_etf_pool:
        log.info("【固定池过滤】固定池为空，跳过过滤")
        return
    dynamic_threshold = g.avg_etf_money_threshold
    log.info("【固定池过滤】使用流动性门槛=日均%.0f万元" % (dynamic_threshold / 1e4))
    TRADE_DAYS_COUNT = 3
    try:
        avg_daily_money = _get_money_avg_series(g.fixed_etf_pool, TRADE_DAYS_COUNT, context)
        if avg_daily_money.empty:
            _warn("【固定池过滤】无法获取成交额数据，跳过过滤")
            g.filtered_fixed_pool = g.fixed_etf_pool[:]
            return
        qualified = avg_daily_money[avg_daily_money > dynamic_threshold]
        new_fixed_pool = qualified.index.tolist()
        removed = set(g.fixed_etf_pool) - set(new_fixed_pool)
        if removed:
            removed_info = []
            for code in removed:
                try:
                    name = getattr(g, 'etf_names_dict', {}).get(code, str(code))
                    money = avg_daily_money.get(code, 0)
                    removed_info.append("%s(%s) %.2f亿" % (name, code, money / 1e8))
                except Exception:
                    removed_info.append(code)
            log.info("【固定池过滤】剔除低流动性ETF(%d只)" % len(removed))
        g.filtered_fixed_pool = new_fixed_pool
        log.info("【固定池过滤】保留高流动性ETF(%d只)" % len(new_fixed_pool))
    except Exception as e:
        _warn("【固定池过滤】异常: %s" % e)
        g.filtered_fixed_pool = g.fixed_etf_pool[:]


def daily_merge_etf_pools(context):
    if not hasattr(g, 'filtered_fixed_pool'):
        g.filtered_fixed_pool = g.fixed_etf_pool[:]
    merged = list(set(g.filtered_fixed_pool + g.dynamic_etf_pool))
    merged.sort()
    log.info("【合并ETF池】开始执行")
    log.info("【合并池统计】固定池: %d只, 动态池: %d只, 合并后: %d只" % (
        len(g.filtered_fixed_pool), len(g.dynamic_etf_pool), len(merged)))
    g.merged_etf_pool = merged
    _update_universe(g.merged_etf_pool)


def calculate_and_log_ranked_etfs(context):
    if not hasattr(g, 'merged_etf_pool') or not g.merged_etf_pool:
        _warn("【动量计算】合并池为空，无法计算")
        g.ranked_etfs_result = []
        return
    final_list = get_final_ranked_etfs(context)
    g.ranked_etfs_result = final_list


def calculate_momentum_score(price_series, lookback_days):
    if len(price_series) < lookback_days + 1:
        return None, None, None
    recent_price_series = price_series[-(lookback_days + 1):]
    y = np.log(recent_price_series)
    x = np.arange(len(y))
    weights = np.linspace(1, 2, len(y))
    W = weights ** 2
    W_sum = np.sum(W)
    x_bar = np.sum(W * x) / W_sum
    y_bar = np.sum(W * y) / W_sum
    dx = x - x_bar
    dy = y - y_bar
    variance_x = np.sum(W * dx ** 2)
    if variance_x == 0:
        return 0, 0, 0
    slope = np.sum(W * dx * dy) / variance_x
    intercept = y_bar - slope * x_bar
    annualized_returns = math.exp(slope * 250) - 1
    y_pred = slope * x + intercept
    ss_res = np.sum(weights * (y - y_pred) ** 2)
    ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot else 0
    momentum_score = annualized_returns * r_squared
    return momentum_score, annualized_returns, r_squared


def calculate_all_metrics_for_etf(etf, etf_name, hist_closes, hist_volumes, current_price, today_vol, context):
    try:
        price_series = np.append(hist_closes, current_price)
        if len(price_series) < g.lookback_days * 0.8:
            return None
        momentum_score, annualized_returns, r_squared = calculate_momentum_score(price_series, g.lookback_days)
        if momentum_score is None:
            return None
        passed_momentum = (g.min_score_threshold <= momentum_score <= g.max_score_threshold)
        volume_ratio = get_volume_ratio(hist_volumes, today_vol, context, g.volume_lookback)

        passed_loss_filter = True
        day_ratios = []
        if len(price_series) >= 4:
            day1 = price_series[-1] / price_series[-2]
            day2 = price_series[-2] / price_series[-3]
            day3 = price_series[-3] / price_series[-4]
            day_ratios = [day1, day2, day3]
            if min(day_ratios) < g.loss:
                passed_loss_filter = False

        passed_r2 = r_squared > g.r2_threshold

        passed_ma = True
        ma_value = None
        if len(price_series) >= g.ma_lookback:
            ma_value = np.mean(price_series[-g.ma_lookback:])
            passed_ma = current_price > ma_value * g.ma_threshold
        else:
            passed_ma = False

        return {
            'etf': etf,
            'etf_name': etf_name,
            'momentum_score': momentum_score,
            'annualized_returns': annualized_returns,
            'r_squared': r_squared,
            'current_price': current_price,
            'volume_ratio': volume_ratio,
            'day_ratios': day_ratios,
            'passed_momentum': passed_momentum,
            'passed_r2': passed_r2,
            'passed_ma': passed_ma,
            'passed_volume': volume_ratio is not None and volume_ratio < g.volume_threshold,
            'passed_loss': passed_loss_filter,
            'ma_value': ma_value,
        }
    except Exception as e:
        _debug("【指标计算】%s %s 计算失败: %s" % (etf, etf_name, e))
        return None


def get_volume_ratio(hist_volumes, today_vol, context, lookback_days=None):
    if lookback_days is None:
        lookback_days = g.volume_lookback
    try:
        if hist_volumes is None or len(hist_volumes) < lookback_days:
            return None
        past_n_days_vol = hist_volumes[-lookback_days:]
        if np.any(np.isnan(past_n_days_vol)) or np.any(past_n_days_vol == 0):
            return None
        avg_volume = np.mean(past_n_days_vol)
        if avg_volume == 0:
            return None
        now = _current_dt(context)
        elapsed_minutes = (now.hour - 9) * 60 + now.minute - 30
        if now.hour >= 13:
            elapsed_minutes -= 90
        elapsed_minutes = max(1, min(elapsed_minutes, 240))
        projected_today_vol = today_vol * (240.0 / elapsed_minutes)
        return projected_today_vol / avg_volume if avg_volume > 0 else 0
    except Exception:
        return None


def check_a_share_weak_period(context):
    today = _today(context)
    indexes = {
        '大盘': '000300.SS',
        '小盘': '399101.SZ',
        '创业板': '399006.SZ',
        '中证A500': '000510.SS'
    }

    exit_lookback = getattr(g, 'weak_exit_ma_lookback', None) or g.weak_period_ma_lookback
    data_lookback = max(g.weak_period_ma_lookback, exit_lookback)

    above_count = 0
    below_count = 0
    exit_above_count = 0
    for name, code in indexes.items():
        df = get_history(data_lookback + 1, '1d', 'close', security_list=code)
        closes = _as_series_values(df)
        if closes is None or len(closes) < data_lookback:
            _warn("📊 【走弱期判断】%s(%s)数据不足，跳过该指数" % (name, code))
            continue
        current_price = closes[-1]
        ma_val = closes[-g.weak_period_ma_lookback:].mean()
        exit_ma_val = closes[-exit_lookback:].mean()
        is_above = current_price > ma_val
        is_below = current_price < ma_val
        is_exit_above = current_price > exit_ma_val
        if is_above:
            above_count += 1
        if is_below:
            below_count += 1
        if is_exit_above:
            exit_above_count += 1
        status_emoji = "⬆️站上" if is_above else ("⬇️低于" if is_below else "➡️持平")
        log.info("📊 【走弱期判断】%s(%s): 收盘%.2f / MA%d %.2f → %s" % (
            name, code, current_price, g.weak_period_ma_lookback, ma_val, status_emoji))

    weak_condition_met = (below_count >= 3)
    exit_condition_met = (exit_above_count >= 3)
    log.info("📊 【走弱期判断】低于MA%d: %d/4, 站上MA%d(退出): %d/4" % (
        g.weak_period_ma_lookback, below_count, exit_lookback, exit_above_count))

    if g.is_a_share_weak and g.weak_start_date is not None:
        try:
            g.weak_days_count = len(get_trade_days(start_date=g.weak_start_date, end_date=today))
        except Exception:
            g.weak_days_count = 0
    else:
        g.weak_days_count = 0
    max_days_exceeded = (g.weak_days_count >= g.max_weak_days)

    if g.is_a_share_weak:
        if max_days_exceeded:
            log.info("🔔 【走弱期退出】已达到最大持续天数%d个交易日，强制退出" % g.max_weak_days)
            g.is_a_share_weak = False
            g.weak_start_date = None
            g.weak_days_count = 0
        elif exit_condition_met:
            log.info("🟢 【走弱期退出】满足退出条件，退出走弱期")
            g.is_a_share_weak = False
            g.weak_start_date = None
            g.weak_days_count = 0
        elif weak_condition_met:
            g.weak_start_date = today
            g.weak_days_count = 0
            log.info("🟡 【走弱期延续】再次触发进入条件，重置计数器")
        else:
            log.info("🔴 【走弱期中】已持续%d/%d个交易日" % (g.weak_days_count, g.max_weak_days))
    else:
        if weak_condition_met:
            log.info("🔴 【走弱期进入】触发进入条件，进入大A走弱期")
            g.is_a_share_weak = True
            g.weak_start_date = today
            g.weak_days_count = 0
        else:
            log.info("🟢 【正常期中】未满足进入条件")

    status_emoji = "🔴" if g.is_a_share_weak else "🟢"
    status_str = "%s 最终状态: 走弱期=%s" % (status_emoji, g.is_a_share_weak)
    if g.is_a_share_weak:
        status_str += " (已持续%d/%d个交易日)" % (g.weak_days_count, g.max_weak_days)
    # 原聚宽 record(走弱期状态=1/0) 在 PTrade 用日志替代
    log.info("📊 【走弱期判断】%s" % status_str)
    return g.is_a_share_weak


def apply_filters(metrics_list):
    steps = [
        ('动量得分', lambda m: m['passed_momentum'], True),
        ('R²', lambda m: m['passed_r2'], g.enable_r2_filter and not g.is_a_share_weak),
        ('均线', lambda m: m['passed_ma'], g.enable_ma_filter and g.is_a_share_weak),
        ('成交量', lambda m: m['passed_volume'], g.enable_volume_check),
        ('短期风控', lambda m: m['passed_loss'], g.enable_loss_filter),
    ]
    filtered = metrics_list[:]
    for name, condition, is_enabled in steps:
        if is_enabled:
            filtered = [m for m in filtered if condition(m)]
    return filtered


def _get_today_volume(context, security):
    """当日累计成交量（分钟线求和；PTrade 快照 volume 为单根分钟量，不能直接用）"""
    try:
        mdf = get_history(241, '1m', 'volume', security_list=security, include=True)
        vals = _as_series_values(mdf)
        if vals is None or len(vals) == 0:
            return 0.0
        # 只保留今天
        today = _today(context)
        mask = np.array([d.date() == today for d in mdf.index])
        if mask.any():
            vals = vals[mask]
        vals = vals[~np.isnan(vals)]
        return float(vals.sum()) if len(vals) else 0.0
    except Exception:
        return 0.0


def get_final_ranked_etfs(context):
    all_metrics = []
    etf_set = list(g.merged_etf_pool)
    log.info("【动量得分计算】使用合并池，合计%d只ETF" % len(etf_set))
    log.info("【当前状态】%s" % ('🔴 大A走弱期' if g.is_a_share_weak else '🟢 大A正常期'))
    lookback = max(g.lookback_days, g.volume_lookback, g.ma_lookback) + 20
    today = _today(context)
    current_data = _cd()
    safe_lookback = lookback + 20
    close_df = _wide(get_history(safe_lookback, '1d', 'close', security_list=etf_set, fq='pre'))
    volume_df = _wide(get_history(safe_lookback, '1d', 'volume', security_list=etf_set))
    if close_df is None or close_df.empty:
        _warn("【动量计算】无法获取历史价格数据")
        return []
    # 当日累计成交量：批量分钟线求和（PTrade 快照 volume 为单根分钟量，不可直接累计）
    today_vols = _get_today_volumes(context, etf_set)
    close_pivot = close_df
    volume_pivot = volume_df
    # ========== 遍历ETF计算动量得分 ==========
    skipped_no_minute = []
    _refresh_halt_status(etf_set, context)
    for etf in etf_set:
        try:
            obj = current_data.get(etf)
            if _is_halted(etf, context):
                continue
            if is_temporarily_suspended(etf, context):
                _debug("%s %s 盘中临时停牌，跳过计算" % (etf, get_security_name(etf)))
                continue
            if etf not in close_pivot.columns:
                continue
            raw_closes = close_pivot[etf].values
            if volume_pivot is None:
                valid_mask = ~np.isnan(raw_closes)
            else:
                raw_volumes = volume_pivot[etf].values
                valid_mask = (~np.isnan(raw_volumes)) & (raw_volumes > 0)
            hist_closes = raw_closes[valid_mask]
            hist_volumes = raw_volumes[valid_mask]
            hist_closes = hist_closes[-lookback:]
            hist_volumes = hist_volumes[-lookback:]
            if len(hist_closes) < g.lookback_days:
                continue
            etf_name = get_security_name(etf)
            current_price = _current_price(etf, context)
            today_vol = today_vols.get(etf, 0)
            metrics = calculate_all_metrics_for_etf(etf, etf_name, hist_closes, hist_volumes, current_price, today_vol, context)
        except RuntimeError as e:
            skipped_no_minute.append((etf, get_security_name(etf), str(e)))
            _warn("⚠️ %s %s 分钟数据获取失败，跳过: %s" % (etf, get_security_name(etf), e))
            continue
        if metrics:
            if metrics['etf'] in {m['etf'] for m in all_metrics}:
                continue
            all_metrics.append(metrics)
    if skipped_no_minute:
        _warn("⚠️ 共%d只ETF因分钟数据缺失被跳过:" % len(skipped_no_minute))
        for code, name, reason in skipped_no_minute:
            _warn("  - %s %s: %s" % (code, name, reason))
    for item in all_metrics:
        score = item.get('momentum_score')
        if pd.isna(score) or (isinstance(score, float) and np.isnan(score)):
            item['momentum_score'] = float('-inf')
    # 按动量得分排序
    all_metrics.sort(key=lambda x: x.get('momentum_score', float('-inf')), reverse=True)
    # ========== 第一步：输出所有ETF排序表格 ==========
    log_buffer = []
    log_buffer.append("")
    log_buffer.append(">>> 第一步：所有ETF按动量得分从大到小排序 <<<")
    for m in all_metrics[:100]:
        def fmt_status(value_str, passed):
            return "%s %s" % (value_str, '✅' if passed else '❌')
        score_str = "%.4f" % m['momentum_score'] if m['momentum_score'] != float('-inf') else "nan"
        r2_str = "%.3f" % m['r_squared'] if not pd.isna(m['r_squared']) else "nan"
        vol_val = "%.2f" % m['volume_ratio'] if m['volume_ratio'] is not None else "N/A"
        min_ratio = min(m['day_ratios']) if m['day_ratios'] else 'N/A'
        loss_val = "%.4f" % min_ratio if isinstance(min_ratio, float) and not pd.isna(min_ratio) else str(min_ratio)
        ma_str = "MA%d: %.2f" % (g.ma_lookback, m['ma_value']) if m['ma_value'] is not None else "MA:N/A"
        line = (
            "%s %s: "
            "动量得分: %s，"
            "R²: %s，"
            "均线: %s，"
            "成交量比值: %s，"
            "短期风控: %s" % (
                m['etf'], m['etf_name'],
                fmt_status(score_str, m['passed_momentum']),
                fmt_status(r2_str, m['passed_r2']),
                fmt_status(ma_str, m['passed_ma']),
                fmt_status(vol_val, m['passed_volume']),
                fmt_status(loss_val, m['passed_loss']),
            )
        )
        log_buffer.append(line)
    # ========== 第二步：应用过滤条件 ==========
    filtered_list = apply_filters(all_metrics)
    filtered_list.sort(key=lambda x: x.get('momentum_score', float('-inf')), reverse=True)
    # 完整过滤后排名（供买入时涨停/停牌 fallback 使用：首选目标买不进时顺延下一名）
    g.ranked_candidates_full = filtered_list
    top_10 = filtered_list[:10]
    log_buffer.append("")
    log_buffer.append(">>> 第二步：符合全部过滤条件的ETF按动量得分从大到小排序(前10名) <<<")
    if top_10:
        for m in top_10:
            def fmt_status(value_str, passed):
                return "%s %s" % (value_str, '✅' if passed else '❌')
            score_str = "%.4f" % m['momentum_score'] if m['momentum_score'] != float('-inf') else "nan"
            r2_str = "%.3f" % m['r_squared'] if not pd.isna(m['r_squared']) else "nan"
            vol_val = "%.2f" % m['volume_ratio'] if m['volume_ratio'] is not None else "N/A"
            min_ratio = min(m['day_ratios']) if m['day_ratios'] else 'N/A'
            loss_val = "%.4f" % min_ratio if isinstance(min_ratio, float) and not pd.isna(min_ratio) else str(min_ratio)
            ma_str = "MA%d: %.2f" % (g.ma_lookback, m['ma_value']) if m['ma_value'] is not None else "MA:N/A"
            line = (
                "%s %s: "
                "动量得分: %s，"
                "R²: %s，"
                "均线: %s，"
                "成交量比值: %s，"
                "短期风控: %s" % (
                    m['etf'], m['etf_name'],
                    fmt_status(score_str, m['passed_momentum']),
                    fmt_status(r2_str, m['passed_r2']),
                    fmt_status(ma_str, m['passed_ma']),
                    fmt_status(vol_val, m['passed_volume']),
                    fmt_status(loss_val, m['passed_loss']),
                )
            )
            log_buffer.append(line)
    else:
        log_buffer.append("（无符合条件的ETF）")
        full_log = "\n".join(log_buffer)
        log.info(full_log)
        return []
    # ========== 第三步：确定候选池 ==========
    score_key = 'momentum_score'
    if len(top_10) >= g.holdings_num:
        reference_score = top_10[g.holdings_num - 1].get(score_key, float('-inf'))
        ratio = g.score_threshold_ratio if not g.is_a_share_weak else 1.0
        score_threshold = reference_score * ratio
        log_buffer.append("")
        log_buffer.append(">>> 第三步：选取动量得分≥第%d名(%s)得分%.4f×%.2f=%.4f的ETF <<<" % (
            g.holdings_num, top_10[g.holdings_num - 1]['etf_name'], reference_score,
            g.score_threshold_ratio, score_threshold))
        candidate_pool = [item for item in top_10 if item.get(score_key, float('-inf')) >= score_threshold]
    else:
        log_buffer.append("")
        log_buffer.append(">>> 第三步：前10名不足%d只，全部作为候选池 <<<" % g.holdings_num)
        candidate_pool = top_10[:]
    log_buffer.append("【候选池】共%d只ETF（按动量得分排序）：" % len(candidate_pool))
    for i, item in enumerate(candidate_pool):
        log_buffer.append("  %d. %s(%s) %s: %.4f" % (i + 1, item['etf_name'], item['etf'], score_key, item.get(score_key, 0)))
    # ========== 第四步：跨资产双持仓选择 ==========
    log_buffer.append("")
    log_buffer.append(">>> 第四步：跨资产双持仓选择 <<<")
    current_holdings = list(_positions_map().keys())
    log_buffer.append("当前持仓ETF：%s" % current_holdings)
    final_result = select_cross_asset_dual(
        current_holdings, filtered_list, score_key, log_buffer)
    log_buffer.append("==================================================")
    full_log = "\n".join(log_buffer)
    log.info(full_log)
    return final_result


def select_cross_asset_dual(current_holdings, filtered_list, score_key, log_buffer):
    """跨资产双持仓选择(自适应权重):
    - slot0 = 全池动量第一;现有 top10 持仓且得分≥第一×0.9 时保留
    - slot1 = 另一资产大类动量第一,需动量≥floor;现有同类持仓≥类首×0.85 保留
    - 权重按动量比自适应: 强腿多得(弱腿仅小仓),避免半仓一个 bet 摊薄收益
    """
    filtered_sorted = sorted(filtered_list,
                             key=lambda x: x.get(score_key, float('-inf')), reverse=True)
    if not filtered_sorted:
        log_buffer.append("【双持仓选择】无过滤后候选，空仓")
        g.target_weights = [1.0]
        return []
    if getattr(g, 'holdings_num', 1) == 1:
        g.target_weights = [1.0]
        return filtered_sorted[:1]

    global_set = set(getattr(g, 'global_etf_pool', []))
    slot1_floor = getattr(g, 'cross_slot1_floor', 0.0)
    slot1_retain = getattr(g, 'cross_slot1_retain_ratio', 0.85)
    adapt = getattr(g, 'cross_adaptive', True)
    log_buffer.append("【双持仓选择】slot1下限=%.2f 自适应=%s" % (slot1_floor, adapt))

    def _is_global(code):
        return code in global_set

    top = filtered_sorted[0]
    slot0 = top
    top10_dict = {m['etf']: m for m in filtered_sorted[:10]}
    held_scored = []
    for h in current_holdings:
        m = top10_dict.get(h)
        if m is not None:
            held_scored.append(m)
    if held_scored:
        best_held = max(held_scored, key=lambda x: x.get(score_key, float('-inf')))
        if best_held.get(score_key, float('-inf')) >= top.get(score_key, float('-inf')) * 0.9:
            slot0 = best_held
            log_buffer.append("【保留 slot0】%s(%s) 得分%.4f ≥ 第一×0.9" % (
                best_held['etf_name'], best_held['etf'], best_held.get(score_key, 0)))

    other_class = [m for m in filtered_sorted
                   if _is_global(m['etf']) != _is_global(slot0['etf'])
                   and m.get(score_key, float('-inf')) >= slot1_floor]
    slot1 = None
    if other_class:
        other_top = other_class[0]
        other_top_score = other_top.get(score_key, float('-inf'))
        slot1 = other_top
        for m in other_class:
            if m['etf'] in current_holdings and m.get(score_key, float('-inf')) >= other_top_score * slot1_retain:
                slot1 = m
                log_buffer.append("【保留 slot1】%s(%s) 得分%.4f" % (
                    m['etf_name'], m['etf'], m.get(score_key, 0)))
                break
    if slot1 is None:
        g.target_weights = [1.0]
        log_buffer.append("【双持仓选择】slot1 空缺 → 退化为单持仓: %s(%s)" % (
            slot0['etf_name'], slot0['etf']))
        return [slot0]
    if slot1.get(score_key, float('-inf')) > slot0.get(score_key, float('-inf')):
        slot0, slot1 = slot1, slot0
        log_buffer.append("【双持仓选择】slot1 动量反超，交换 slot0/slot1")
    if adapt:
        s0 = max(float(slot0.get(score_key, 0.0)), 0.01)
        s1 = max(float(slot1.get(score_key, 0.0)), 0.01)
        w1 = s0 / (s0 + s1)
        w1 = max(0.5, min(getattr(g, 'cross_weight_cap', 0.85), w1))
        w2 = round(1.0 - w1, 3)
        w1 = round(w1, 3)
    else:
        w1, w2 = 0.5, 0.5
    g.target_weights = [w1, w2]
    log_buffer.append("【双持仓选择】权重 %.3f/%.3f" % (w1, w2))
    log_buffer.append("【最终目标】共2只ETF：")
    for i, item in enumerate([slot0, slot1]):
        cls = '全球/海外' if _is_global(item['etf']) else '大A/港股'
        log_buffer.append("  %d. %s(%s) [%s] %s: %.4f" % (
            i + 1, item['etf_name'], item['etf'], cls, score_key, item.get(score_key, 0)))
    return [slot0, slot1]


def execute_sell_trades(context):
    log.info("========== 卖出操作开始 ==========")
    ranked_etfs = getattr(g, 'ranked_etfs_result', [])
    target_etfs = []

    if ranked_etfs:
        for metrics in ranked_etfs[:g.holdings_num]:
            target_etfs.append(metrics['etf'])
            log.info("确定最终目标: %s %s" % (metrics['etf'], metrics['etf_name']))
    else:
        if check_defensive_etf_available(context):
            target_etfs = [g.defensive_etf]
            etf_name = get_security_name(g.defensive_etf)
            log.info("🛡️ 确定最终目标(防御模式): %s %s" % (g.defensive_etf, etf_name))
        else:
            log.info("💤 无最终目标(空仓模式)")
            target_etfs = []

    g.target_etfs_list = target_etfs
    current_positions = _positions_map()
    target_set = set(target_etfs)
    sell_count = 0

    for security, position in current_positions.items():
        if _pos_amount(position) > 0 and security not in target_set:
            security_name = get_security_name(security)
            success = smart_order_target_value(security, 0, context)
            if success:
                sell_count += 1
                log.info("✅ 已成功卖出: %s %s" % (security, security_name))

    log.info("本次共计划卖出%d只ETF。" % sell_count)
    log.info("========== 卖出操作完成 ==========")


def execute_buy_trades(context):
    log.info("========== 买入操作开始 ==========")
    target_etfs = g.target_etfs_list

    if not target_etfs:
        log.info("根据计算的结果，今日无目标ETF，保持空仓")
        log.info("========== 买入操作完成 ==========")
        return

    current_positions = _positions_map()
    etfs_to_buy = [etf for etf in target_etfs if etf not in current_positions]
    actual_holding_count = len(current_positions)
    max_buy_count = max(0, g.holdings_num - actual_holding_count)
    num_etfs_to_buy = min(len(etfs_to_buy), max_buy_count)

    if num_etfs_to_buy <= 0:
        log.info("当前实际持仓数量(%d)已达到或超过目标(%d)，无需买入" % (actual_holding_count, g.holdings_num))
        log.info("========== 买入操作完成 ==========")
        return

    etfs_to_buy = etfs_to_buy[:num_etfs_to_buy]
    log.info("当前实际持仓: %d只, 目标持仓: %d只, 本次计划买入: %d只" % (
        actual_holding_count, g.holdings_num, num_etfs_to_buy))

    # 完整过滤后排名（首选目标买不进时顺延下一名，避免空仓）
    ranked_full = getattr(g, 'ranked_candidates_full', []) or []
    fallback_order = [m['etf'] for m in ranked_full]
    bought_etfs = set(current_positions)  # 已持有/已买入的不再重复买

    # 修复：动态分配资金，避免可用现金为负
    for i in range(num_etfs_to_buy):
        remaining_cash = _get_available_cash(context)
        if remaining_cash < g.min_money:
            log.info("可用现金 %.2f 不足最小交易额 %.2f，停止买入" % (remaining_cash, g.min_money))
            break

        remaining_to_buy = num_etfs_to_buy - i
        # 槽位加权分配：新买入槽位目标市值 = 总资产 × 槽位权重(target_weights);
        # 单持仓退化 weights=[1.0] -> 全仓。最后一笔用剩余现金消化余量。
        slot = actual_holding_count + i
        total_value = _get_total_value(context)
        _weights = getattr(g, 'target_weights', None)
        if i == num_etfs_to_buy - 1:
            target_value_for_this_etf = remaining_cash
        elif _weights and len(_weights) > 1:
            _w = _weights[slot] if slot < len(_weights) else 1.0 / g.holdings_num
            target_value_for_this_etf = min(remaining_cash, total_value * _w)
        else:
            target_value_for_this_etf = remaining_cash // remaining_to_buy

        # 最后一笔可使用剩余全部现金，但确保不小于最小交易额
        if target_value_for_this_etf < g.min_money and remaining_cash >= g.min_money:
            target_value_for_this_etf = remaining_cash

        # 候选顺序：首选目标(etfs_to_buy[i]) 优先，随后顺延完整排名
        primary = etfs_to_buy[i] if i < len(etfs_to_buy) else None
        candidates = []
        if primary is not None and primary not in bought_etfs:
            candidates.append(primary)
        for cand in fallback_order:
            if cand not in bought_etfs and cand not in candidates:
                candidates.append(cand)

        success = False
        for cand in candidates:
            log.info("为 %s 分配目标金额: %.2f 元 (剩余现金 %.2f, 待买数量 %d)" % (
                cand, target_value_for_this_etf, remaining_cash, remaining_to_buy))
            if smart_order_target_value(cand, target_value_for_this_etf, context):
                log.info("✅ ETF %s 下单成功" % cand)
                bought_etfs.add(cand)
                success = True
                break
            else:
                log.info("⚠️ %s 买入失败(涨停/停牌等)，顺延下一名候选" % cand)
        if not success:
            log.info("❌ 本轮无可用候选ETF可买入(均涨停/停牌)，停止买入")

    log.info("========== 买入操作完成 ==========")


def is_temporarily_suspended(security, context, minute_count=10):
    """
    判断ETF是否盘中临时停牌
    通过检查最近N分钟是否有成交来判断，若无成交则视为临时停牌
    """
    try:
        # 获取最近N分钟的分钟线数据
        minute_data = get_history(minute_count, '1m', 'volume', security_list=security, include=True)
        vals = _as_series_values(minute_data)
        # 无数据或数据为空，视为停牌
        if vals is None or len(vals) == 0:
            return True
        vals = vals[~np.isnan(vals)]
        if len(vals) == 0:
            return True
        # 最近N分钟成交量都为0，视为临时停牌
        if np.all(vals == 0):
            return True
        return False
    except Exception as e:
        _debug("临时停牌检测异常 %s: %s" % (security, e))
        return False  # 异常时默认认为正常，避免误判


def smart_order_target_value(security, target_value, context):
    """
    智能下单：根据目标市值调整持仓，处理停牌、涨跌停、最小交易金额、T+1
    """
    name = get_security_name(security)
    price = _current_price(security, context)
    if not price:
        log.info("%s %s 无实时行情数据，跳过交易" % (security, name))
        return False
    # ========== 1. 全天停牌检测 ==========
    if _is_halted(security, context):
        log.info("%s %s 全天停牌，跳过交易" % (security, name))
        return False
    # ========== 2. 盘中临时停牌检测 ==========
    if is_temporarily_suspended(security, context):
        log.info("%s %s 盘中临时停牌，跳过交易" % (security, name))
        return False
    # ========== 3. 买入时使用预估成交价（包含佣金+滑点）计算股数 ==========
    estimated_price = price
    if target_value > 0:
        buy_commission_rate = 0.0001   # 买入佣金
        slippage_rate = 0.0001         # 滑点
        estimated_price = price * (1 + buy_commission_rate + slippage_rate)
        target_amount = int(target_value / estimated_price)
        target_amount = (target_amount // 100) * 100
        if target_amount <= 0:
            target_amount = 100
        # 二次校验：用实时可用现金和预估成交价(含佣金+滑点)严格限制（兜底）
        max_shares = int(_get_available_cash(context) / estimated_price)
        max_shares = (max_shares // 100) * 100
        if max_shares < target_amount:
            target_amount = max_shares
        if target_amount <= 0:
            log.info("%s %s: 现金不足买100股，跳过" % (security, name))
            return False
    else:
        target_amount = 0
    cur_pos = _get_position(security)
    cur_amount = _pos_amount(cur_pos)
    diff = target_amount - cur_amount
    # ========== 4. 涨跌停检测（统一：涨停跌停都不交易；回测经日线字段获取，拿不到则跳过） ==========
    high_limit, low_limit = _limit_prices(security, context)
    if high_limit and price >= high_limit:
        log.info("%s %s 当前涨停，跳过交易" % (security, name))
        return False
    if low_limit and price <= low_limit:
        log.info("%s %s 当前跌停，跳过交易" % (security, name))
        return False
    trade_val = abs(diff) * price
    if 0 < trade_val < g.min_money:
        log.info("%s %s 交易金额%.2f < %d，跳过" % (security, name, trade_val, g.min_money))
        return False
    # ========== 5. T+1检查（仅卖出时） ==========
    if diff < 0:
        closeable = _pos_avail(cur_pos)
        if closeable == 0:
            log.info("%s %s 当天买入不可卖出(T+1)" % (security, name))
            return False
        diff = -min(abs(diff), closeable)
    # ========== 6. 执行下单 ==========
    if diff != 0:
        order_result = order(security, diff)
        if order_result:
            if diff > 0:
                log.info("📥 买入 %s %s 数量%d 价格%.3f (预估含成本价: %.3f)" % (
                    security, name, abs(diff), price, estimated_price))
            else:
                log.info("📤 卖出 %s %s 数量%d 价格%.3f" % (security, name, abs(diff), price))
            return True
        else:
            _warn("下单失败: %s %s，数量%d" % (security, name, diff))
            return False
    return False


def minute_level_stop_loss(context):
    if not g.use_fixed_stop_loss:
        return
    current_time = _current_dt(context).strftime('%H:%M')
    if not (('09:40' < current_time < '10:29') or ('10:40' < current_time < '11:30') or ('13:00' < current_time < '14:57')):
        return
    for security, position in _positions_map().items():
        if _pos_amount(position) <= 0 or _pos_avail(position) <= 0:
            if security in g._profit_protected:
                del g._profit_protected[security]
            if security in g._peak_price:
                del g._peak_price[security]
            continue
        current_price = _current_price(security, context)
        if current_price <= 0:
            continue
        cost_price = _pos_cost(position)
        if cost_price <= 0:
            continue
        stop_threshold = g.fixedStopLossThreshold
        profit_ratio = current_price / cost_price - 1
        # D3 高位回落止盈（v5.4）：曾浮盈≥阈值后，从持仓峰值回落≥幅度则卖出
        if getattr(g, 'enable_take_profit', False):
            peak = g._peak_price.get(security, current_price)
            if current_price > peak:
                peak = current_price
                g._peak_price[security] = peak
            if peak / cost_price - 1 >= getattr(g, 'take_profit_ratio', 0.08) \
                    and current_price <= peak * (1 - getattr(g, 'take_profit_pullback', 0.03)):
                security_name = get_security_name(security)
                log.info("🎯 【高位回落止盈】%s %s 从峰值%.3f回落至%.3f，锁定盈利 %.2f%%" % (
                    security, security_name, peak, current_price, profit_ratio * 100))
                smart_order_target_value(security, 0, context)
                continue
        if getattr(g, 'enable_profit_protect', False):
            if profit_ratio >= getattr(g, 'profit_protect_trigger', 0.10):
                g._profit_protected[security] = True
            if g._profit_protected.get(security, False):
                stop_threshold = getattr(g, 'profit_protect_stop', 1.0)
        if current_price <= cost_price * stop_threshold:
            security_name = get_security_name(security)
            loss_percent = (current_price / cost_price - 1) * 100
            stop_label = "成本×%.2f" % stop_threshold if stop_threshold >= 1.0 else "成本×%.0f%%" % g.fixedStopLossThreshold
            log.info("🚨 【分钟级固定止损】%s %s 触发止损(%s)，亏损: %.2f%%" % (
                security, security_name, stop_label, loss_percent))
            smart_order_target_value(security, 0, context)


def check_defensive_etf_available(context):
    current_data = _cd()
    defensive_etf = g.defensive_etf
    obj = current_data.get(defensive_etf)
    if obj is None:
        return False
    price = getattr(obj, 'lastPrice', 0) or 0
    if price == 0:
        return False
    if _is_halted(defensive_etf, context):
        log.info("防御性ETF %s 今日停牌" % defensive_etf)
        return False
    high_limit, low_limit = _limit_prices(defensive_etf, context)
    if high_limit and price >= high_limit:
        log.info("防御性ETF %s 当前涨停" % defensive_etf)
        return False
    if low_limit and price <= low_limit:
        log.info("防御性ETF %s 当前跌停" % defensive_etf)
        return False
    return True
