# 聚宽五福ETF轮动策略（可直接粘贴到策略编辑器运行）
# 五标的：沪深300ETF / 中证500ETF / 创业板ETF / 国债ETF / 黄金ETF
# 动量比较选最强，每日调仓
# 迁移自 quant-daydayup：strategy/wufu_etf_rotation.py（保留原出处与聚宽语法）

ETF_POOL = ['510300.XSHG', '510500.XSHG', '159915.XSHE', '511010.XSHG', '518880.XSHG']
LOOKBACK = 20


def init(context):
    context.universe = ETF_POOL


def _momentum(sec):
    df = get_price(sec, count=LOOKBACK, frequency='daily')
    if df is None or len(df) < 2:
        return -1.0
    closes = df['close'] if 'close' in df else df.iloc[:, -1]
    return float(closes.iloc[-1] / closes.iloc[0] - 1)


def handle(context):
    best, best_m = None, -1.0
    for sec in context.universe:
        m = _momentum(sec)
        if m > best_m:
            best, best_m = sec, m
    target = best or context.universe[0]
    for sec in context.universe:
        order_target_percent(sec, 1.0 if sec == target else 0.0)


def period(context):
    handle(context)


run_daily(period, 'open')
