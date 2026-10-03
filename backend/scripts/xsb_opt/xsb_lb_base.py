# 强势股连板策略 — 本地引擎适配版（源自 e6dbd5c6 聚宽克隆链）
# 原版：rinke《强势股连板策略》 <- voidcui《短线打板策略》 <- 十九点九 复刻版
#
# 本地适配（jqcompat 无对应数据/API 的部分）：
# 1. 概念层移除：本地无概念板块数据（get_concepts 为空），热门概念交集选股退化为
#    直接使用全市场连板>=MIN_LB 池；保留一字板占比过滤与连板数排序。
# 2. VOL5 因子本地化：get_factor_values 不存在，改为 attribute_history 5日均量升序。
# 3. order_target/LimitOrderStyle 本地等价实现（order_shares 增量下单）。
# 4. filter_paused_stock 退化为直通：涨停判定 close==high_limit 天然排除停牌股。
#
# 调优参数全部可用环境变量覆盖（P_*），便于脚本批量扫参。

import os

from jqdata import *  # noqa: F401,F403
import datetime as dt  # noqa: F401
import pandas as pd  # noqa: F401
import numpy as np  # noqa: F401


def _env_f(name, default):
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_i(name, default):
    try:
        return int(float(os.getenv(name, default)))
    except (TypeError, ValueError):
        return int(default)


def _env_b(name, default):
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ('1', 'true', 'yes', 'on')


def dbg(msg):
    """调试输出：P_QUIET=0 时直印 stdout（log.info 走内部日志通道看不到）"""
    if not _env_b('P_QUIET', True):
        print('[dbg] %s' % msg, flush=True)


# ========================== 调优参数（env 可覆盖） ==========================
P_TOP_N = _env_i('P_TOP_N', 5)              # 最终选股数量上限
P_MIN_LB = _env_i('P_MIN_LB', 2)            # 最小连板数
P_GAP_MIN = _env_f('P_GAP_MIN', 0.02)       # 开盘涨幅下限
P_GAP_MAX = _env_f('P_GAP_MAX', 0.08)       # 开盘涨幅上限
P_EXTREME_MAX = _env_f('P_EXTREME_MAX', 0.66)  # 一字板占比上限
P_PULLBACK_TP = _env_f('P_PULLBACK_TP', 0.05)  # 非龙头回撤止盈阈值
P_BUY_PCT_STRONG = _env_i('P_BUY_PCT_STRONG', 1)  # 强市：总资产/该值 = 单票上限
P_BUY_PCT_WEAK = _env_i('P_BUY_PCT_WEAK', 2)      # 弱市同上（2=最高半仓）
P_SINGLE_CAP = _env_f('P_SINGLE_CAP', 0.50)  # 单票占净值上限
P_FRIDAY_HALF = _env_b('P_FRIDAY_HALF', True)      # 周五不开新仓+持仓减半
P_PRE_HOLIDAY_SKIP = _env_b('P_PRE_HOLIDAY_SKIP', True)  # 节前不开新仓
P_MARKET_FILTER = _env_b('P_MARKET_FILTER', True)  # 沪深300 MA 走弱降仓开关
P_WEAK_SKIP = _env_b('P_WEAK_SKIP', False)   # 弱市日不开新仓（只按规则卖出）
P_LOSS_STOP = _env_f('P_LOSS_STOP', 0.0)     # 10:30 亏损止损阈值（0=任何亏损即卖）
P_OPEN_TOL = _env_f('P_OPEN_TOL', 0.0)       # 开盘低开卖出容忍度（0=任何低开即卖）
P_VOL5_ASC = _env_b('P_VOL5_ASC', True)      # VOL5 排序：True 量小优先


class LimitOrderStyle:
    """本地引擎按信号价成交，限价样式仅占位。"""

    def __init__(self, price=None):
        self.price = price


def order_target(context, stock, target_amount):
    """聚宽 order_target 等价：按目标股数增量下单（只能卖出可卖部分）。"""
    try:
        pos = context.portfolio.positions[stock]
        cur = pos.total_amount or 0
    except Exception:
        cur = 0
    cur = int(cur)
    target_amount = int(target_amount)
    if target_amount == cur:
        return None
    if target_amount < cur:
        sellable = int(getattr(context.portfolio.positions[stock],
                               'closeable_amount', 0) or 0) if cur > 0 else 0
        delta = -min(cur - target_amount, sellable)
        if delta == 0:
            return None
    else:
        delta = target_amount - cur
    return order_shares(stock, delta)


def initialize(context):
    """初始化函数：设置策略参数、全局变量和定时任务"""
    set_option('use_real_price', True)
    set_option('avoid_future_data', True)
    set_option('match_by_signal', True)
    if _env_b('P_QUIET', True):
        log.set_level('system', 'error')
    # 交易成本：佣金万3，印花税千1，最低佣金5元
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003,
                             close_commission=0.0003, close_today_commission=0,
                             min_commission=5), type='stock')

    g.buy_pct = P_BUY_PCT_WEAK
    g.top_n = P_TOP_N
    g.target_list = []
    g.position_high = {}
    g.dragon_count = {}

    run_daily(sell_check_open, '09:28')
    run_daily(get_stock_list, '09:25:00')
    run_daily(buy, '09:30')
    run_daily(update_position_high, '09:35')
    run_daily(update_position_high, '10:00')
    run_daily(sell_pullback, '10:00')
    run_daily(sell_check_morning, '10:30')
    run_daily(update_position_high, '11:00')
    run_daily(update_position_high, '13:30')
    run_daily(update_position_high, '14:00')
    run_daily(sell_pullback, '14:00')
    run_daily(sell_check_afternoon, '14:30')
    run_daily(print_position_info, '15:10')


def update_position_high(context):
    """更新持仓股票的最高价记录"""
    current_data = get_current_data()
    for stock in list(context.portfolio.positions.keys()):
        pos = context.portfolio.positions[stock]
        if pos.total_amount <= 0:
            continue
        current_price = current_data[stock].last_price
        if current_price is None or current_price <= 0:
            continue
        if stock not in g.position_high or current_price > g.position_high[stock]:
            g.position_high[stock] = current_price


def sell_pullback(context):
    """检查并执行回撤止盈（非龙头股）"""
    current_data = get_current_data()
    for stock in list(context.portfolio.positions.keys()):
        pos = context.portfolio.positions[stock]
        if pos.closeable_amount <= 0:
            continue
        if stock not in g.position_high:
            continue
        avg_cost = pos.avg_cost
        current_price = current_data[stock].last_price
        high_price = g.position_high[stock]
        if avg_cost <= 0 or current_price <= 0 or high_price <= avg_cost:
            continue

        pullback = (high_price - current_price) / high_price
        dragon_count = g.dragon_count.get(stock, 1)

        if dragon_count >= 3:
            continue

        if pullback >= P_PULLBACK_TP:
            if current_price > current_data[stock].low_limit:
                log.info('[Pullback TP] %s cost:%.2f high:%.2f now:%.2f pullback:%.1f%%' % (
                    stock, avg_cost, high_price, current_price, pullback * 100))
                order_target(context, stock, 0)
                g.position_high.pop(stock, None)
                g.dragon_count.pop(stock, None)


def get_stock_list(context):
    """核心选股：全市场连板股 -> 一字板占比过滤 -> 开盘涨幅过滤 -> VOL5 升序 topN"""
    date = context.previous_date
    date = transform_date(date, 'str')

    initial_list = prepare_stock_list(date)
    hl_list = get_hl_stock(initial_list, date)
    ccd_hl = get_continue_count_df(hl_list, date, 20) if len(hl_list) != 0 else \
        pd.DataFrame(index=[], data={'count': [], 'extreme_count': []})
    ccd_lb = ccd_hl[ccd_hl['count'] >= P_MIN_LB] if len(ccd_hl) != 0 else ccd_hl

    if market_signal(context):
        g.buy_pct = P_BUY_PCT_STRONG
        dbg("market good, pct=%d (max exposure %.0f%%)" % (g.buy_pct, 100.0 / g.buy_pct))
    else:
        g.buy_pct = P_BUY_PCT_WEAK
        dbg("market weak, pct=%d (max exposure %.0f%%)" % (g.buy_pct, 100.0 / g.buy_pct))
        if P_WEAK_SKIP:
            g.target_list = []
            return

    # 本地无概念数据：跳过热门概念层，直接用连板池 + 一字板占比过滤
    if len(ccd_lb) != 0:
        final_df = ccd_lb[ccd_lb['extreme_count'] / ccd_lb['count'] <= P_EXTREME_MAX].copy()
        final_df = final_df.sort_values('count', ascending=False)
    else:
        final_df = pd.DataFrame(index=[], data={'count': [], 'extreme_count': []})
    dbg('date=%s initial=%d hl=%d lb>=2=%d lb pool after extreme filter: %d' % (
        date, len(initial_list), len(hl_list), len(ccd_lb), len(final_df)))
    lt = list(final_df.index)

    condition_dct = {}
    current_data = get_current_data()
    for s in lt:
        try:
            hist = attribute_history(security=s, count=1, unit='1d',
                                     fields=('close', 'high_limit'),
                                     skip_paused=False, df=False, fq='pre')
            pct_chg = (current_data[s].day_open - hist['close'][-1]) / hist['close'][-1]
            if (pct_chg >= P_GAP_MIN) and (pct_chg <= P_GAP_MAX) and \
                    (current_data[s].day_open <= current_data[s].high_limit - 0.03):
                condition_dct[s] = condition_dct.get(s, '') + 'buy'
        except Exception:
            pass
    stock_list = list(condition_dct.keys())
    dbg('after gap filter: %d %s' % (len(stock_list), stock_list))

    df = get_vol5_filter_df(context, stock_list)
    stock_list = list(df.index)
    stock_list = stock_list[:g.top_n]
    dbg('after vol5 (top%d): %s targets=%s' % (g.top_n, stock_list, g.target_list))

    g.dragon_count = {}
    for s in stock_list:
        if s in final_df.index:
            g.dragon_count[s] = int(final_df.loc[s, 'count'])
        else:
            g.dragon_count[s] = 1

    hold_list = list(context.portfolio.positions)
    g.target_list = [x for x in stock_list if x not in hold_list]


def buy(context):
    """执行买入操作（含周五/节前最后交易日过滤）"""
    current_data = get_current_data()

    today = context.current_dt.date()
    is_friday = (today.weekday() == 4)
    is_pre_holiday = is_pre_holiday_day(context) if P_PRE_HOLIDAY_SKIP else False

    if is_friday or is_pre_holiday:
        reason = '周五' if is_friday else '节前最后交易日'
        log.info('[%s] 当日不开新仓' % reason)

        if is_friday and P_FRIDAY_HALF:
            for stock in list(context.portfolio.positions.keys()):
                pos = context.portfolio.positions[stock]
                closeable = pos.closeable_amount
                if closeable <= 0:
                    continue
                half_amount = int(closeable / 2 / 100) * 100
                if half_amount < 100:
                    log.info('[周五减半] %s 半仓不足100股, 清仓(可卖%d股)' % (stock, closeable))
                    order_target(context, stock, 0)
                else:
                    log.info('[周五减半] %s 可卖%d股, 目标半仓%d股' % (stock, closeable, half_amount))
                    order_target(context, stock, half_amount)
                g.position_high.pop(stock, None)
                g.dragon_count.pop(stock, None)
        elif is_friday:
            log.info('[周五] 减仓关闭，仅不开新仓')

        g.target_list = []
        return

    if len(g.target_list) > 0:
        value1 = context.portfolio.total_value / g.buy_pct
        uncapped_value = min(context.portfolio.available_cash, value1) / len(g.target_list)
        single_stock_cap = context.portfolio.total_value * P_SINGLE_CAP
        value = min(uncapped_value, single_stock_cap)
        if value < uncapped_value:
            log.info('[concentration cap] targets:%d value:%.2f -> %.2f' % (
                len(g.target_list), uncapped_value, value))
        for s in g.target_list:
            if 'ST' in current_data[s].name or '退' in current_data[s].name:
                log.info('skip %s (%s) ST' % (s, current_data[s].name))
                continue
            if context.portfolio.available_cash / current_data[s].last_price > 100:
                order_value(s, value)
                dbg('buy %s value %.2f' % (s, value))
                g.position_high[s] = current_data[s].day_open


def sell_check_open(context):
    """开盘时检查卖出条件：低开即卖"""
    current_data = get_current_data()
    for stock in context.portfolio.positions:
        amount = context.portfolio.positions[stock].closeable_amount
        if amount == 0:
            continue
        open_price = current_data[stock].day_open
        hist = attribute_history(stock, 1, '1d', ['close'], skip_paused=False)
        if len(hist) > 0:
            prev_close = hist['close'][0]
        else:
            prev_close = current_data[stock].high_limit / 1.1

        if open_price < prev_close * (1 - P_OPEN_TOL) and open_price > current_data[stock].low_limit:
            log.info('【open stop】%s open:%.2f prev:%.2f sell' % (stock, open_price, prev_close))
            order_target(context, stock, 0)
            g.position_high.pop(stock, None)
            g.dragon_count.pop(stock, None)
        elif open_price == current_data[stock].low_limit:
            log.info('【open stop】%s limit down, cannot sell' % stock)


def sell_check_morning(context):
    """上午10:30检查卖出条件"""
    current_data = get_current_data()
    for stock in context.portfolio.positions:
        amount = context.portfolio.positions[stock].closeable_amount
        if amount == 0:
            continue
        curr_price = current_data[stock].last_price
        high_limit = current_data[stock].high_limit
        low_limit = current_data[stock].low_limit
        avg_cost = context.portfolio.positions[stock].avg_cost
        is_limit = curr_price >= (high_limit - 0.01)

        if is_limit:
            log.info('【10:30 hold】%s limit up' % stock)
            continue

        if curr_price == low_limit:
            log.info('%s limit down, cannot sell' % stock)
            continue

        if stock in g.position_high and avg_cost > 0:
            high_price = g.position_high[stock]
            pullback = (high_price - curr_price) / high_price if high_price > 0 else 0
            dragon_count = g.dragon_count.get(stock, 1)
            if dragon_count < 3 and pullback >= P_PULLBACK_TP and curr_price > avg_cost:
                log.info('【10:30 pullback TP】%s pullback %.1f%% sell' % (stock, pullback * 100))
                order_target(context, stock, 0)
                g.position_high.pop(stock, None)
                g.dragon_count.pop(stock, None)
                continue

        pnl_ratio = (curr_price - avg_cost) / avg_cost
        if avg_cost <= 0:
            continue
        if pnl_ratio < P_LOSS_STOP:
            log.info('【10:30 stop】%s loss %.2f%% sell' % (stock, pnl_ratio * 100))
            order_target(context, stock, 0)
            g.position_high.pop(stock, None)
            g.dragon_count.pop(stock, None)
        else:
            target_amount = int(amount / 2 / 100) * 100
            if target_amount < 100:
                order_target(context, stock, 0)
                log.info('【10:30 TP】%s profit %.2f%% too few shares, sell all' % (
                    stock, pnl_ratio * 100))
            else:
                order_target(context, stock, target_amount)
                log.info('【10:30 TP】%s profit %.2f%% sell half' % (stock, pnl_ratio * 100))
            g.position_high.pop(stock, None)
            g.dragon_count.pop(stock, None)


def sell_check_afternoon(context):
    """下午14:30检查卖出条件：尾盘不涨停即卖"""
    current_data = get_current_data()
    for stock in context.portfolio.positions:
        amount = context.portfolio.positions[stock].closeable_amount
        if amount == 0:
            continue
        curr_price = current_data[stock].last_price
        high_limit = current_data[stock].high_limit
        low_limit = current_data[stock].low_limit
        avg_cost = context.portfolio.positions[stock].avg_cost
        is_limit = curr_price >= (high_limit - 0.01)

        if is_limit:
            log.info('【14:30 hold】%s limit up overnight' % stock)
        else:
            if curr_price == low_limit:
                log.info('%s limit down, cannot sell' % stock)
                continue

            if stock in g.position_high and avg_cost > 0:
                high_price = g.position_high[stock]
                pullback = (high_price - curr_price) / high_price if high_price > 0 else 0
                dragon_count = g.dragon_count.get(stock, 1)
                if dragon_count < 3 and pullback >= P_PULLBACK_TP and curr_price > avg_cost:
                    log.info('【14:30 pullback TP】%s pullback %.1f%% sell' % (
                        stock, pullback * 100))
                    order_target(context, stock, 0)
                    g.position_high.pop(stock, None)
                    g.dragon_count.pop(stock, None)
                    continue

            log.info('【14:30 clear】%s not limit up, sell' % stock)
            order_target(context, stock, 0)
            g.position_high.pop(stock, None)
            g.dragon_count.pop(stock, None)


# ==================== 辅助函数 ====================

def is_pre_holiday_day(context):
    """判断当前交易日是否为节假日前的最后一个交易日（间隔 >=4 自然日）"""
    today = context.current_dt.date()
    future_days = get_trade_days(start_date=today, end_date=today + dt.timedelta(days=20))
    if len(future_days) <= 1:
        return False
    next_trade_day = future_days[1]
    gap_days = (next_trade_day - today).days
    return gap_days >= 4


def transform_date(date, date_type):
    """日期格式转换：支持str、datetime、date类型互转"""
    if type(date) == str:
        str_date = date
        dt_date = dt.datetime.strptime(date, '%Y-%m-%d')
        d_date = dt_date.date()
    elif type(date) == dt.datetime:
        str_date = date.strftime('%Y-%m-%d')
        dt_date = date
        d_date = date.date()
    elif type(date) == dt.date:
        str_date = date.strftime('%Y-%m-%d')
        dt_date = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date = date
    dct = {'str': str_date, 'dt': dt_date, 'd': d_date}
    return dct[date_type]


def get_shifted_date(date, days, days_type='T'):
    """获取偏移日期：days_type='T'为交易日，'N'为自然日"""
    d_date = transform_date(date, 'd')
    yesterday = d_date + dt.timedelta(-1)
    if days_type == 'N':
        shifted_date = yesterday + dt.timedelta(days + 1)
    if days_type == 'T':
        all_trade_days = [i.strftime('%Y-%m-%d') for i in list(get_all_trade_days())]
        if str(yesterday) in all_trade_days:
            shifted_date = all_trade_days[all_trade_days.index(str(yesterday)) + days + 1]
        else:
            for i in range(100):
                last_trade_date = yesterday - dt.timedelta(i)
                if str(last_trade_date) in all_trade_days:
                    shifted_date = all_trade_days[all_trade_days.index(str(last_trade_date)) + days + 1]
                    break
    return str(shifted_date)


def filter_new_stock(initial_list, date, days=50):
    """过滤上市未满指定天数的新股"""
    d_date = transform_date(date, 'd')
    return [stock for stock in initial_list
            if d_date - get_security_info(stock).start_date > dt.timedelta(days=days)]


def filter_st_stock(initial_list, date):
    """过滤ST股票"""
    str_date = transform_date(date, 'str')
    if get_shifted_date(str_date, 0, 'N') != get_shifted_date(str_date, 0, 'T'):
        str_date = get_shifted_date(str_date, -1, 'T')
    df = get_extras('is_st', initial_list, start_date=str_date, end_date=str_date, df=True)
    df = df.T
    df.columns = ['is_st']
    df = df[df['is_st'] == False]  # noqa: E712
    return list(df.index)


def filter_kcbj_stock(initial_list):
    """过滤科创板(688)、北交所(4/8开头)股票"""
    return [stock for stock in initial_list
            if stock[0] != '4' and stock[0] != '8' and stock[:2] != '68']


def filter_paused_stock(initial_list, date):
    """停牌过滤：本地日线无 paused 字段，退化为直通。

    涨停判定 close==high_limit 天然要求当日有成交，停牌股进不了 hl_list。
    """
    return initial_list


def filter_name_stock(initial_list):
    """过滤名称中含ST、※ST、退的股票"""
    current_data = get_current_data()
    filtered_list = []
    for s in initial_list:
        name = current_data[s].name
        if 'ST' not in name and '※ST' not in name and '退' not in name:
            filtered_list.append(s)
    return filtered_list


def filter_stock_list(stock_list, date):
    """综合过滤股票：过滤科创板、新股、ST、停牌、名称含ST/退"""
    if not stock_list:
        return []
    stock_list = filter_kcbj_stock(stock_list)
    stock_list = filter_new_stock(stock_list, date)
    stock_list = filter_st_stock(stock_list, date)
    stock_list = filter_paused_stock(stock_list, date)
    stock_list = filter_name_stock(stock_list)
    return stock_list


def prepare_stock_list(date):
    """准备初始股票池：全市场股票并过滤"""
    initial_list = get_all_securities('stock', date).index.tolist()
    initial_list = filter_stock_list(initial_list, date)
    return initial_list


def market_signal(context):
    """市场情绪信号：沪深300 MA5>MA20 且价格在 MA5 上方"""
    prices = attribute_history('000300.XSHG', 60, '1d', fields=['close'], skip_paused=True)
    if len(prices) < 60:
        return False
    ma5 = prices['close'].rolling(window=5).mean()
    ma20 = prices['close'].rolling(window=20).mean()
    return (ma5.iloc[-1] > ma20.iloc[-1] and prices['close'].iloc[-1] > ma5.iloc[-1])


def get_vol5_filter_df(context, stock_list):
    """VOL5 因子本地化：5日均量（attribute_history），按升序/降序排序"""
    if len(stock_list) == 0:
        return pd.DataFrame(index=[], data={'score': []})
    rows = []
    for s in stock_list:
        try:
            h = attribute_history(s, 5, '1d', fields=['volume'], skip_paused=True)
            if len(h) < 5:
                continue
            vol = float(h['volume'].iloc[-5:].mean())
            if vol <= 0:
                continue
            rows.append((s, vol))
        except Exception:
            continue
    df = pd.DataFrame(rows, columns=['code', 'score']).set_index('code')
    df = df.sort_values(by='score', ascending=P_VOL5_ASC)
    return df


def get_hl_stock(initial_list, date):
    """获取指定日期的涨停股票列表（复权口径下用容差比较，严格相等必失配）"""
    df = get_price(initial_list, end_date=date, frequency='daily',
                   fields=['close', 'high_limit'], count=1,
                   panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna()
    if _env_b('P_DEBUG_HL', False):
        from rqalpha.environment import Environment as _Env
        try:
            _dp = _Env.get_instance().data_proxy
            _s0 = initial_list[0]
            dbg('probe: s0=%s ins=%s' % (_s0, _dp.get_instrument(_s0)))
        except Exception as _e:
            dbg('probe err: %r' % _e)
        try:
            _one = get_price(initial_list[0], end_date=date, frequency='daily',
                             fields=['close', 'high_limit'], count=1,
                             panel=False, fill_paused=False, skip_paused=False)
            dbg('probe single get_price: %s' % _one.to_dict('records'))
        except Exception as _e:
            dbg('probe single err: %r' % _e)
        _few = get_price(initial_list[:3], end_date=date, frequency='daily',
                         fields=['close', 'high_limit'], count=1,
                         panel=False, fill_paused=False, skip_paused=False)
        dbg('probe batch3: rows=%d codes=%s' % (len(_few), sorted(set(_few.code)) if len(_few) else []))
    tol = df['high_limit'] * 0.0015
    df = df[df['close'] >= df['high_limit'] - tol]
    return list(df.code)


def get_continue_count_df(hl_list, date, watch_days):
    """获取连板计数DataFrame：统计每只股票的连续涨停天数和一字板天数"""
    if not hl_list:
        return pd.DataFrame(columns=['count', 'extreme_count'])
    df = get_price(hl_list, end_date=date, frequency='daily',
                   fields=['close', 'high_limit', 'low'],
                   count=watch_days, panel=False, fill_paused=False, skip_paused=False)
    if df.empty:
        return pd.DataFrame(columns=['count', 'extreme_count'])
    results = []
    for stock in hl_list:
        stock_df = df[df['code'] == stock].copy()
        stock_df = stock_df.sort_values('time', ascending=False)
        stock_df.reset_index(drop=True, inplace=True)
        consecutive_count = 0
        extreme_count = 0
        for i in range(len(stock_df)):
            row = stock_df.iloc[i]
            hl = row['high_limit']
            if hl <= 0 or hl != hl:
                break
            tol = hl * 0.0015
            if row['close'] >= hl - tol:
                consecutive_count += 1
                if row['low'] >= hl - tol:
                    extreme_count += 1
            else:
                break
        if consecutive_count > 0:
            results.append({'code': stock, 'count': consecutive_count,
                            'extreme_count': extreme_count})
    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df.set_index('code', inplace=True)
    return result_df


def print_position_info(context):
    """收盘后打印持仓信息"""
    for position in list(context.portfolio.positions.values()):
        log.info('code:%s cost:%.2f price:%.2f ret:%.2f%% amount:%d value:%.2f' % (
            position.security, position.avg_cost, position.price,
            100 * (position.price / position.avg_cost - 1) if position.avg_cost else 0,
            position.total_amount, position.value))
    log.info('==========')
