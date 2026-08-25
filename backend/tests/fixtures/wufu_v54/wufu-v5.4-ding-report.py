# 克隆自聚宽文章：https://www.joinquant.com/post/74243
# 标题：【五福闹新春】v5.2-已解密。别再被13:10狙击了，快跑
# 作者：烟花三月ETF
# v5.3（本地改进版）：A1 盈利保护止损 / A2 持仓宽容（调仓惰性）/ A3 走弱期退出确认，均可独立开关。
# v5.4（胜率导向）：D1 锁盈止损（保护线 1.0→成本×X） / D2 买入过滤收紧 / D3 高位回落止盈，均可独立开关。
# v5.4 优化（2026-08-12）：A3 退出均线 20→15（弱市反弹更快回补 A 股池，避免错过反弹被锁在全球池）。
# v5.4-ding（2026-08-13）：克隆 42f91131 版；所有买入/卖出/止损走 log.notify 推钉钉，账户止损见 Matcher。
# v5.4.1（2026-08-21）：数据缺失保护——持仓在池内但未参与动量计算（取数瞬时失败被
#   静默跳过，08-17~08-21 连续误换仓根因）时保守保留持仓 + 告警/推钉钉，不基于残缺排名换仓。
# v5.4-report（2026-08-25）：克隆 wufu-v5.4-ding；每天 11:30/13:01 预买卖报告
#   （预测 13:10 调仓：预计买入 + 最可能卖出前3），log.notify 落库+推钉钉。

import numpy as np
import math
import pandas as pd
from jqdata import *
from datetime import datetime, date, timedelta

import warnings
# 抑制 pandas 2.3 对 Series 位置索引（s[-1] 等）的弃用警告：
# 策略沿用聚宽原版索引写法，大量 s[-1] 触发 FutureWarning，静默处理。
warnings.filterwarnings(
    "ignore", message=r".*treating keys as positions is deprecated.*"
)


def initialize(context):
    set_option("avoid_future_data", True)                               # 避免未来数据：防止回测中使用未来信息导致虚高收益
    set_option("use_real_price", True)                                  # 开启动态复权：正确处理分红、送股等事件，使用真实价格
    set_slippage(PriceRelatedSlippage(0.0001), type="fund")             # 设置百分比滑点：买入时价格提高0.01%，卖出时价格降低0.01%
    set_order_cost(OrderCost(open_tax=0, close_tax=0, open_commission=0.0001, close_commission=0.0001, close_today_commission=0.0001, min_commission=5), type="fund")  # 设置交易费用：基金类ETF免印花税，佣金万分之一（0.01%），最低5元
    log.set_level('order', 'error')                                     # 订单日志级别：只显示错误信息
    log.set_level('system', 'error')                                    # 系统日志级别：只显示错误信息
    log.set_level('strategy', 'info')                                   # 策略日志级别：显示普通信息及以上
    log.info("【五福闹新春】v5.4（胜率优化版）！")

    # ==================== ETF池定义 ====================
    # 全球/海外ETF池（含大宗商品和海外市场ETF）
    g.global_etf_pool = [
#大宗商品ETF：
        '518880.XSHG',  # (黄金ETF) [ETF]-日均成交额：51.35亿元-上市日期：2013-07-29
        '501018.XSHG',  # (南方原油) [LOF]-日均成交额：24.38亿元-上市日期：2016-06-28
        '161226.XSHE',  # (国投白银LOF) [LOF]-日均成交额：5.44亿元-上市日期：2015-08-17
        '159985.XSHE',  # (豆粕ETF华夏) [ETF]-日均成交额：4.63亿元-上市日期：2019-12-05
        '159980.XSHE',  # (有色ETF大成) [ETF]-日均成交额：3.84亿元-上市日期：2019-12-24
#海外ETF：       
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
    # 中国ETF池（含港股、指数、行业ETF）
    g.china_etf_pool = [
#港股ETF：
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
#指数ETF：        
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
#行业ETF：
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

    g.holdings_num = 1
    g.defensive_etf = "511880.XSHG"
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
    g._entry_date = {}                                   # ding：code -> 首次买入日期（持仓天数通知用）
    g._daily_traded = False                              # ding：当日是否已下单（无换仓通知用）

    set_benchmark("510300.XSHG")
# ==================== 定时任务 ====================
    run_daily(morning_routine, time='09:00')            # 09:00 晨间流水线：持仓检查→回撤监控→流动性阈值计算
    run_daily(check_weak_period_daily, time='09:40')    # 09:40 走弱期判断+池子更新
    run_daily(afternoon_routine, time='13:10')          #  动量计算与排序（需早于卖出时间）         
    run_daily(sell_routine, time='13:10')               #  卖出流水线（需早于买入时间）
    run_daily(buy_routine, time='13:10')                #  买入流水线
    run_daily(pre_trade_report, time='11:30')           # 预买卖报告：午间场（预测 13:10 调仓）
    run_daily(pre_trade_report, time='13:01')           # 预买卖报告：尾盘场（距决策9分钟）
    run_daily(reset_daily_flags, time='15:10')          # 15:10 收盘流水线：重置价格缓存
    run_daily(minute_level_stop_loss, time='every_bar') # 分钟级固定止损
    
    log.info(f"""
【策略参数初始化完成】
=== ETF池配置 ===
- 全球/海外ETF池: {len(g.global_etf_pool)}只
- 国内ETF池: {len(g.china_etf_pool)}只
- 固定池合计: {len(g.fixed_etf_pool)}只
=== 大A走弱期判定 ===
- MA均线周期: {g.weak_period_ma_lookback}日
- 进入条件: 至少3/4指数低于MA{g.weak_period_ma_lookback}
- 退出条件: 至少3/4指数站上MA{g.weak_period_ma_lookback}
- 最长持续: {g.max_weak_days}个交易日
=== 动量得分过滤 ===
- 周期: {g.lookback_days}天
- 得分阈值: [{g.min_score_threshold}, {g.max_score_threshold}]
- 调仓系数: {g.score_threshold_ratio}
=== 过滤条件 ===
- 正常期 R²过滤: {'启用' if g.enable_r2_filter else '禁用'} (阈值>{g.r2_threshold:.1f})
- 走弱期 均线过滤: {'启用' if g.enable_ma_filter else '禁用'} (MA{g.ma_lookback}×{g.ma_threshold})
- 通用 成交量过滤: {'启用' if g.enable_volume_check else '禁用'} (近{g.volume_lookback}日均量比<{g.volume_threshold:.1f})
- 通用 短期风控: {'启用' if g.enable_loss_filter else '禁用'} (近3日单日跌幅<{1-g.loss:.0%})
=== 止损机制 ===
- 分钟级固定比例止损: {'启用' if g.use_fixed_stop_loss else '禁用'} (成本价×{g.fixedStopLossThreshold:.0%})
- A1 盈利保护止损: {'启用' if g.enable_profit_protect else '禁用'} (浮盈≥{g.profit_protect_trigger:.0%}后止损上移至成本×{g.profit_protect_stop})
=== v5.3 其他改进 ===
- A2 持仓宽容: {'启用' if g.hold_buffer < 1.0 else '禁用'} (持仓得分≥候选池门槛×{g.hold_buffer}时保留，回测验证为负贡献)
- A3 走弱期退出确认: {'启用' if g.weak_exit_ma_lookback != g.weak_period_ma_lookback else '禁用'} (退出需3/4指数站上MA{g.weak_exit_ma_lookback})
=== v5.4 胜率改进 ===
- D2a 动量下限: {g.min_score_threshold}  D2b R²阈值: {g.r2_threshold}  D2c 量比上限: {g.volume_threshold}  D2d 单日跌幅上限: {1-g.loss:.0%}
- D3 高位回落止盈: {'启用' if g.enable_take_profit else '禁用'} (曾浮盈≥{g.take_profit_ratio:.0%}后从峰值回落≥{g.take_profit_pullback:.0%}卖出)
=== 其他配置 ===
- 持仓数量: {g.holdings_num}只
- 防御ETF: {g.defensive_etf}
- 最小交易额: {g.min_money}元
- 基准: 510300.XSHG
""")


def check_weak_period_daily(context):
    check_a_share_weak_period(context)
    midday_routine(context)


def morning_routine(context):
    log.info("★" * 80)
    log.info("▶️ 【晨间流水线】启动...")
    g._daily_traded = False                              # ding：每日交易前重置无换仓标志
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
        log.info(f"🔴 【走弱期池更新】仅对全球/海外ETF池进行流动性过滤...")
        filter_global_pool_by_volume(context)
        log.info(f"【走弱期池更新完成】过滤后全球池: {len(g.filtered_global_pool)}只")
    else:
        log.info(f"🟢 【正常期池更新】执行动态池更新、固定池过滤、合并池...")
        log.info("【动态池更新】更新行业ETF动态池（各行业流动性最佳ETF）...")
        update_sector_pool(context)
        log.info("【固定池过滤】过滤固定ETF池流动性...")
        filter_fixed_pool_by_volume(context)
        log.info("【合并池】合并固定池与动态池...")
        daily_merge_etf_pools(context)
        log.info(f"【正常期池更新完成】合并池: {len(g.merged_etf_pool)}只")
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
        log.info(f"🔴 【大A走弱期】使用过滤后全球/海外ETF池，共{len(g.merged_etf_pool)}只")
    else:
        log.info(f"🟢 【大A正常期】使用合并池，共{len(g.merged_etf_pool)}只")
    # 预加载分钟线缓存（避免午盘动量计算时逐标的回源）
    try:
        from app.quant.jqengine.datasource.manager import get_data_manager
        dm = get_data_manager()
        dm.preload_minute_for_pool(g.merged_etf_pool, context.current_dt)
        log.info(f"📦 【分钟线预加载】已为 {len(g.merged_etf_pool)} 只ETF 预热缓存")
    except Exception as e:
        log.warning(f"分钟线预加载失败: {e}")
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
    if not g._daily_traded:
        holding_count = len(context.portfolio.positions)
        log.notify(f"🈳 今日无换仓：持有{holding_count}只，维持当前仓位")

# ==================== 预买卖报告（v5.4-report）====================
PRE_REPORT_TOP_SELLS = 3       # 卖出候选最多展示前 3


def _is_replay_past(context):
    """是否处于"过去日期"的历史补跑（跳过）；今天的盘中补跑返回 False（允许触发）。"""
    try:
        lag_seconds = (datetime.now() - context.current_dt).total_seconds()
    except Exception:
        return False
    return lag_seconds > 300 and context.current_dt.date() < datetime.now().date()


def pre_trade_report(context):
    """11:30 / 13:01 预买卖报告：预测 13:10 调仓（预计买入 + 最可能卖出前3）。

    直接复用 get_final_ranked_etfs(quiet=True)，与 13:10 正式管线完全同口径
    （含持仓宽容/数据缺失保护）。结果走 log.notify：落 sim_logs 并推钉钉。
    """
    if _is_replay_past(context):
        return  # 历史日补跑：不浪费全池计算、不产生无用日志
    if not getattr(g, 'merged_etf_pool', None):
        log.info("【预买卖报告】合并池未就绪，跳过本次预报告")
        return
    tag = context.current_dt.strftime('%H:%M')
    log.info(f"▶️ 【预买卖报告 @{tag}】启动...")
    try:
        from app.quant.jqengine.datasource.manager import get_data_manager
        dm = get_data_manager()
        dm.preload_minute_for_pool(g.merged_etf_pool, context.current_dt)
        log.info(f"📦 【预买卖报告】已预热 {len(g.merged_etf_pool)} 只分钟缓存")
    except Exception as e:
        log.warning(f"【预买卖报告】分钟线预加载失败（回退逐标的取数）: {e}")
    ranked = get_final_ranked_etfs(context, quiet=True)
    msg = _build_pre_trade_message(context, tag, ranked)
    log.notify(msg)
    log.info(f"⏸️ 【预买卖报告 @{tag}】执行完毕！")


def _short_code(code):
    return code.split('.')[0]


def _pre_report_target_codes(context, ranked):
    """与 execute_sell_trades 同口径的目标集推导：前 N 或防御兜底。"""
    if ranked:
        return [m['etf'] for m in ranked[:g.holdings_num]], False
    if check_defensive_etf_available(context):
        return [g.defensive_etf], True
    return [], True


def _build_pre_trade_message(context, tag, ranked):
    """组装预买卖报告文本（纯展示逻辑，可单测）。

    卖出候选排序＝"最可能被卖"优先：无过滤排名者最先，其余按完整过滤排名
    位置从后往前（排名越差越先卖）；截断前 PRE_REPORT_TOP_SELLS。
    """
    holdings = [sec for sec, pos in context.portfolio.positions.items()
                if pos.total_amount > 0]
    regime = '🔴 大A走弱期' if getattr(g, 'is_a_share_weak', False) else '🟢 大A正常期'
    pool_size = len(getattr(g, 'merged_etf_pool', []) or [])
    head = (f"📋 预买卖报告 {context.current_dt.strftime('%m-%d')} {tag}"
            f"（预测 13:10 调仓 | {regime} | 池{pool_size}只）")

    target_codes, defensive_mode = _pre_report_target_codes(context, ranked)
    hold_set = set(holdings)
    target_set = set(target_codes)
    lines = [head]
    if defensive_mode:
        lines.append("🛡️ 排名为空：走防御模式" if target_codes
                     else "🛡️ 排名空且防御ETF不可用：走空仓模式")

    buys = [c for c in target_codes if c not in hold_set]
    if buys:
        buy_strs = [f"{_short_code(c)} {get_security_name(c)}" for c in buys]
        lines.append("📥 预计买入：" + " → ".join(buy_strs))
    else:
        lines.append("📥 预计买入：无")

    full_rank = getattr(g, 'ranked_candidates_full', []) or []
    rank_pos = {m['etf']: i for i, m in enumerate(full_rank)}
    score_of = {}
    for m in full_rank:
        score_of.setdefault(m['etf'], m.get('momentum_score'))
    total = max(len(full_rank), 1)
    assessed = set(getattr(g, '_assessed_codes', []) or [])
    pool_set = set(getattr(g, 'merged_etf_pool', []) or [])

    sells, unevaluated = [], []
    for sec in holdings:
        if sec in target_set:
            continue
        if assessed and sec not in assessed and sec in pool_set:
            unevaluated.append(f"{sec} {get_security_name(sec)}")
            continue
        p = rank_pos.get(sec)
        s = score_of.get(sec)
        pos_str = f"排名{p + 1}/{total}" if p is not None else "未入过滤排名"
        s_str = f"{s:.4f}" if isinstance(s, (int, float)) and s == s else "N/A"
        sells.append((sec, get_security_name(sec), pos_str, s_str, p))

    if sells:
        sells.sort(key=lambda x: (x[4] is not None, -(x[4] if x[4] is not None else 0)))
        n = min(PRE_REPORT_TOP_SELLS, len(sells))
        lines.append(f"📤 预计卖出（最可能前{n}）：")
        emojis = ['1️⃣', '2️⃣', '3️⃣']
        for i, (sec, nm, pos_str, s_str, _p) in enumerate(sells[:PRE_REPORT_TOP_SELLS]):
            lines.append(f"{emojis[i]} {_short_code(sec)} {nm}（{pos_str}，动量{s_str}）")
    elif not unevaluated:
        lines.append(f"✅ 持仓全部在目标内（{len(holdings)}只），13:10 预计不动")

    if unevaluated:
        lines.append(f"⚠️ 未参与评估（数据缺失保护，13:10 不会强卖）：{'、'.join(unevaluated)}")
    return "\n".join(lines)


def reset_daily_flags(context):
    g.cache_date = None
    g.yesterday_close_cache = {}
    log.info("🔄 收盘缓存重置完成")


def check_positions(context):
    current_data = get_current_data()
    for security in context.portfolio.positions:
        position = context.portfolio.positions[security]
        if position.total_amount > 0:
            security_name = get_security_name(security)
            log.info(f"📊 【持仓检查】{security} {security_name}, 数量: {position.total_amount}, 成本: {position.avg_cost:.3f}, 当前价: {position.price:.3f}")
            if current_data[security].paused:
                log.info(f"⚠️ {security} {security_name} 今日停牌")


def monitor_drawdown(context):
    try:
        current_value = context.portfolio.total_value
        if current_value > g.max_portfolio_value:
            g.max_portfolio_value = current_value
        if g.max_portfolio_value > 0:
            current_drawdown = (g.max_portfolio_value - current_value) / g.max_portfolio_value
            if current_drawdown >= g.drawdown_threshold:
                record = {
                    'date': context.current_dt.strftime('%Y-%m-%d'),
                    'drawdown': current_drawdown,
                    'portfolio_value': current_value,
                    'max_value': g.max_portfolio_value,
                    'is_weak': g.is_a_share_weak
                }
                positions_info = []
                for security in context.portfolio.positions:
                    position = context.portfolio.positions[security]
                    if position.total_amount > 0:
                        security_name = get_security_name(security)
                        positions_info.append(f"{security_name}:{position.total_amount}股")
                record['positions'] = positions_info
                g.drawdown_records.append(record)
                log.info(f"【回撤预警】回撤达到 {current_drawdown:.2%} (阈值: {g.drawdown_threshold:.0%})")
                log.info(f"  当前净值: {current_value:,.0f}  |  最高净值: {g.max_portfolio_value:,.0f}")
                log.info(f"  大A状态: {'走弱期' if g.is_a_share_weak else '正常期'}")
                log.info(f"  持仓: {', '.join(positions_info) if positions_info else '空仓'}")
    except Exception as e:
        log.error(f"【回撤监控】计算异常: {e}")


def _anomalous_etf_days(daily_totals, daily_counts):
    """返回 3 日全市场 ETF 成交额中明显偏低的异常日（只数或金额 < 另两天较大者 50%）。

    数据回源不完整（如 08-13 仅 225 只、1469 亿，正常 ~1658 只、~4000 亿）时，只数与
    金额同时掉到正常 1/3 以下；正常日只数波动 <2%。返回 [] 表示无异常。
    """
    days = list(daily_totals.index)
    anomaly = []
    for day in days:
        others = [d for d in days if d != day]
        max_other_count = max(daily_counts.get(d, 0) for d in others)
        max_other_money = max(daily_totals[d] for d in others)
        count = daily_counts.get(day, 0)
        money = daily_totals[day]
        if (max_other_count and count < max_other_count * 0.5) \
                or (max_other_money and money < max_other_money * 0.5):
            anomaly.append(day)
    return anomaly


def calculate_global_etf_threshold(context):
    log.info("【全局阈值更新】开始计算全市场ETF流动性门槛")
    try:
        # 缓存全市场 ETF 列表（仅首次获取）
        if not hasattr(g, '_cached_etf_universe') or g._cached_etf_universe is None:
            df_etf = get_all_securities(['etf'], date=context.current_dt)
            g._cached_etf_universe = df_etf.index.tolist()
            log.info(f"全市场ETF总数: {len(g._cached_etf_universe)}只 (已缓存)")
        etf_list = g._cached_etf_universe
        if not etf_list:
            log.warning("未找到任何场内ETF，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        trade_days = get_trade_days(end_date=context.previous_date, count=3)
        start_day = trade_days[0]
        # 直接从 DataManager 缓存读取日线成交额，避免走 get_price 链路
        from app.quant.jqengine.datasource.manager import get_data_manager
        dm = get_data_manager()
        df = dm.get_daily_money_cached(etf_list, end_date=context.previous_date, count=3)
        if df is None or df.empty:
            log.warning("缓存中无成交额数据，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        daily_totals = df.groupby('time')['money'].sum()
        daily_counts = df[df['money'] > 0].groupby('time')['code'].nunique()
        for day, money in daily_totals.items():
            count = daily_counts.get(day, 0)
            log.info(f"  {day.date()} 全市场ETF总成交额: {money/1e8:.2f}亿元 ({count}只ETF有成交)")
        if len(daily_totals) < 3:
            log.warning(f"仅有{len(daily_totals)}个有效交易日，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        anomaly_days = _anomalous_etf_days(daily_totals, daily_counts)
        if anomaly_days:
            for day in anomaly_days:
                money = daily_totals[day]
                count = daily_counts.get(day, 0)
                msg = (f"🚨【成交额异常】{day.date()} 全市场ETF总成交额 {money/1e8:.2f}亿元 "
                       f"({count}只ETF有成交)，明显低于其他两天，疑似数据回源不完整，"
                       f"已剔除该日计算阈值")
                log.error(msg)
                log.notify(msg)
            good = [d for d in daily_totals.index if d not in anomaly_days]
            if len(good) < 2:
                log.warning("剔除异常日后不足2个正常交易日，使用保守阈值1000万")
                g.avg_etf_money_threshold = 10000000
                return
            avg_total_money = daily_totals[good].mean()
            threshold = avg_total_money / g.global_threshold_divisor
            g.avg_etf_money_threshold = threshold
            log.info(f"【全局阈值更新完成】(已剔除异常日) 近{len(good)}日全市场ETF日均总成交额="
                     f"{avg_total_money/1e8:.2f}亿元，阈值={threshold/1e4:.0f}万元({threshold:,.0f}元)")
            return
        avg_total_money = daily_totals.mean()
        threshold = avg_total_money / g.global_threshold_divisor
        g.avg_etf_money_threshold = threshold
        log.info(f"【全局阈值更新完成】近{len(daily_totals)}日全市场ETF日均总成交额={avg_total_money/1e8:.2f}亿元，阈值={threshold/1e4:.0f}万元({threshold:,.0f}元)")
    except Exception as e:
        log.warning(f"计算全局阈值异常: {e}，使用保守阈值1000万")
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
    log.info(f"【全球池过滤】使用流动性门槛=日均{dynamic_threshold/1e4:.0f}万元")
    end_date = context.previous_date
    TRADE_DAYS_COUNT = 3
    try:
        # 直接从 DataManager 缓存读取日线成交额
        from app.quant.jqengine.datasource.manager import get_data_manager
        dm = get_data_manager()
        price_data = dm.get_daily_money_cached(g.global_etf_pool, end_date=end_date, count=TRADE_DAYS_COUNT)
        if price_data is None or price_data.empty:
            log.warning("【全球池过滤】缓存中无成交额数据，使用原始全球池")
            g.filtered_global_pool = g.global_etf_pool[:]
            return
        total_money = price_data.groupby('code')['money'].sum()
        avg_daily_money = total_money / TRADE_DAYS_COUNT
        qualified = avg_daily_money[avg_daily_money > dynamic_threshold]
        new_global_pool = qualified.index.tolist()
        removed = set(g.global_etf_pool) - set(new_global_pool)
        if removed:
            removed_info = []
            for code in removed:
                try:
                    name = getattr(g, 'etf_names_dict', {}).get(code, str(code))
                    money = avg_daily_money.get(code, 0)
                    removed_info.append(f"{name}({code}) {money/1e8:.2f}亿")
                except:
                    removed_info.append(code)
            log.info(f"【全球池过滤】剔除低流动性ETF({len(removed)}只)")
        g.filtered_global_pool = new_global_pool
        sorted_qualified = qualified.sort_values(ascending=False)
        log.info(f"【全球池过滤】保留高流动性ETF({len(new_global_pool)}只)")
    except Exception as e:
        log.warning(f"【全球池过滤】异常: {e}")
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
        df_etf = get_all_securities(['etf'])
        etf_list = df_etf.index.tolist()
        g.etf_names_dict = df_etf['display_name'].to_dict()
    except Exception as e:
        log.warning(f"获取全市场ETF列表失败: {e}")
        return
    
    log.info(f"【动态池更新】全市场ETF总数: {len(etf_list)}只")
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
    log.info(f"【动态池更新】特别组分布: {group_counts}")
    log.info(f"【动态池更新】进入特别组: {len(special_etfs)}只")
    log.info(f"【动态池更新】进入普通组: {len(normal_etfs)}只")
    log.info(f"【动态池更新】排除ETF: {excluded_count}只")
    
    end_date = context.previous_date
    TRADE_DAYS_COUNT = 3
    dynamic_threshold = g.avg_etf_money_threshold
    
    def filter_by_liquidity(etf_codes, group_name):
        if not etf_codes:
            return pd.Series(dtype=float), 0
        try:
            price_data = get_price(etf_codes, end_date=end_date, count=TRADE_DAYS_COUNT, frequency='daily', fields=['money'], panel=False)
            if price_data is None or price_data.empty:
                return pd.Series(dtype=float), len(etf_codes)
            total_money = price_data.groupby('code')['money'].sum()
            avg_daily_money = total_money / TRADE_DAYS_COUNT
            qualified_series = avg_daily_money[avg_daily_money > dynamic_threshold].sort_values(ascending=False)
            filtered_out = len(etf_codes) - len(qualified_series)
            return qualified_series, filtered_out
        except Exception:
            return pd.Series(dtype=float), len(etf_codes)
    
    normal_qualified, normal_filtered_out = filter_by_liquidity(normal_etfs, "普通组")
    special_qualified, special_filtered_out = filter_by_liquidity(special_etfs, "特别组")
    normal_sorted = normal_qualified.index.tolist()
    special_sorted = special_qualified.index.tolist()
    log.info(f"【动态池更新】特别组流动性过滤: {len(special_etfs)}→{len(special_sorted)}只")    
    log.info(f"【动态池更新】普通组流动性过滤: {len(normal_etfs)}→{len(normal_sorted)}只")
    
    if not normal_sorted and not special_sorted:
        log.warning("【动态池更新】无ETF通过流动性过滤")
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
            group_key = f"{matched_group}_{industry_key}"
            if group_key not in special_industry_groups:
                special_industry_groups[group_key] = []
            special_industry_groups[group_key].append({
                'code': code, 'original_name': original_name, 'cleaned_name': cleaned,
                'money': money, 'group_type': matched_group, 'display_group': matched_group
            })
        except Exception:
            continue
    
    final_pool_info = []
    _group_dropped = []
    for industry_key, items in normal_industry_groups.items():
        sorted_items = sorted(items, key=lambda x: x['money'], reverse=True)
        final_pool_info.append(sorted_items[0])
        if len(sorted_items) > 1:
            _group_dropped.append(f"{industry_key}组落选: " + ", ".join(
                f"{i['original_name']}({i['code']}) {i['money']/1e8:.2f}亿"
                for i in sorted_items[1:]))
    for group_key, items in special_industry_groups.items():
        sorted_items = sorted(items, key=lambda x: x['money'], reverse=True)
        final_pool_info.append(sorted_items[0])
        if len(sorted_items) > 1:
            _group_dropped.append(f"[特]{group_key}: " + ", ".join(
                f"{i['original_name']}({i['code']}) {i['money']/1e8:.2f}亿"
                for i in sorted_items[1:]))
    if _group_dropped:
        # 分组落选审计：动态池每组只留成交额最大一只，其余静默排除曾导致
        # 同策略在不同名称映射环境下选出完全不同的候选（08-10 华宝事件）。
        log.info("【动态池分组审计】" + " | ".join(_group_dropped))
    
    final_pool_info_sorted = sorted(final_pool_info, key=lambda x: x['money'], reverse=True)
    top_300 = final_pool_info_sorted[:300]
    g.dynamic_etf_pool = [item['code'] for item in top_300]
    log.info(f"【动态池更新完成】动态池共{len(g.dynamic_etf_pool)}只ETF")
    if len(g.dynamic_etf_pool) <= 10:
        for item in top_300[:10]:
            log.info(f"  {item['code']} {item['original_name']} 日均成交额: {item['money']/1e8:.2f}亿")


def filter_fixed_pool_by_volume(context):
    log.info("【固定池过滤】开始执行")
    if getattr(g, 'avg_etf_money_threshold', None) is None:
        log.info("【固定池过滤】阈值未初始化，立即计算")
        calculate_global_etf_threshold(context)
    if not g.fixed_etf_pool:
        log.info("【固定池过滤】固定池为空，跳过过滤")
        return
    dynamic_threshold = g.avg_etf_money_threshold
    log.info(f"【固定池过滤】使用流动性门槛=日均{dynamic_threshold/1e4:.0f}万元")
    end_date = context.previous_date
    TRADE_DAYS_COUNT = 3
    try:
        price_data = get_price(g.fixed_etf_pool, end_date=end_date, count=TRADE_DAYS_COUNT, frequency='daily', fields=['money'], panel=False)
        if price_data is None or price_data.empty:
            log.warning("【固定池过滤】无法获取成交额数据，跳过过滤")
            g.filtered_fixed_pool = g.fixed_etf_pool[:]
            return
        total_money = price_data.groupby('code')['money'].sum()
        avg_daily_money = total_money / TRADE_DAYS_COUNT
        qualified = avg_daily_money[avg_daily_money > dynamic_threshold]
        new_fixed_pool = qualified.index.tolist()
        removed = set(g.fixed_etf_pool) - set(new_fixed_pool)
        if removed:
            removed_info = []
            for code in removed:
                try:
                    name = getattr(g, 'etf_names_dict', {}).get(code, str(code))
                    money = avg_daily_money.get(code, 0)
                    removed_info.append(f"{name}({code}) {money/1e8:.2f}亿")
                except:
                    removed_info.append(code)
            log.info(f"【固定池过滤】剔除低流动性ETF({len(removed)}只)")
        g.filtered_fixed_pool = new_fixed_pool
        sorted_qualified = qualified.sort_values(ascending=False)
        log.info(f"【固定池过滤】保留高流动性ETF({len(new_fixed_pool)}只)")
    except Exception as e:
        log.warning(f"【固定池过滤】异常: {e}")
        g.filtered_fixed_pool = g.fixed_etf_pool[:]


def daily_merge_etf_pools(context):
    if not hasattr(g, 'filtered_fixed_pool'):
        g.filtered_fixed_pool = g.fixed_etf_pool[:]
    merged = list(set(g.filtered_fixed_pool + g.dynamic_etf_pool))
    merged.sort()
    log.info("【合并ETF池】开始执行")
    log.info(f"【合并池统计】固定池: {len(g.filtered_fixed_pool)}只, 动态池: {len(g.dynamic_etf_pool)}只, 合并后: {len(merged)}只")
    g.merged_etf_pool = merged


def calculate_and_log_ranked_etfs(context):
    if not hasattr(g, 'merged_etf_pool') or not g.merged_etf_pool:
        log.warning("【动量计算】合并池为空，无法计算")
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
    variance_x = np.sum(W * dx**2)
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
        log.debug(f"【指标计算】{etf} {etf_name} 计算失败: {e}")
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
        now = context.current_dt
        elapsed_minutes = (now.hour - 9) * 60 + now.minute - 30
        if now.hour >= 13:
            elapsed_minutes -= 90
        elapsed_minutes = max(1, min(elapsed_minutes, 240))
        projected_today_vol = today_vol * (240.0 / elapsed_minutes)
        return projected_today_vol / avg_volume if avg_volume > 0 else 0
    except Exception:
        return None


def check_a_share_weak_period(context):
    today = context.current_dt.date()
    indexes = {
        '大盘': '000300.XSHG',
        '小盘': '399101.XSHE',
        '创业板': '399006.XSHE',
        '中证A500': '000510.XSHG'
    }
    exit_lookback = getattr(g, 'weak_exit_ma_lookback', None) or g.weak_period_ma_lookback
    data_lookback = max(g.weak_period_ma_lookback, exit_lookback)
    
    above_count = 0
    below_count = 0
    exit_above_count = 0
    for name, code in indexes.items():
        df = attribute_history(code, data_lookback + 1, '1d', ['close'], skip_paused=False)
        if df is None or len(df) < data_lookback:
            log.warning(f"📊 【走弱期判断】{name}({code})数据不足，跳过该指数")
            continue
        current_price = df['close'][-1]
        ma_val = df['close'][-g.weak_period_ma_lookback:].mean()
        exit_ma_val = df['close'][-exit_lookback:].mean()
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
        log.info(f"📊 【走弱期判断】{name}({code}): 收盘{current_price:.2f} / MA{g.weak_period_ma_lookback} {ma_val:.2f} → {status_emoji}")
    
    weak_condition_met = (below_count >= 3)
    exit_condition_met = (exit_above_count >= 3)
    log.info(f"📊 【走弱期判断】低于MA{g.weak_period_ma_lookback}: {below_count}/4, 站上MA{exit_lookback}(退出): {exit_above_count}/4")
    
    if g.is_a_share_weak and g.weak_start_date is not None:
        g.weak_days_count = len(get_trade_days(start_date=g.weak_start_date, end_date=today))
    else:
        g.weak_days_count = 0
    max_days_exceeded = (g.weak_days_count >= g.max_weak_days)
    
    if g.is_a_share_weak:
        if max_days_exceeded:
            log.info(f"🔔 【走弱期退出】已达到最大持续天数{g.max_weak_days}个交易日，强制退出")
            g.is_a_share_weak = False
            g.weak_start_date = None
            g.weak_days_count = 0
        elif exit_condition_met:
            log.info(f"🟢 【走弱期退出】满足退出条件，退出走弱期")
            g.is_a_share_weak = False
            g.weak_start_date = None
            g.weak_days_count = 0
        elif weak_condition_met:
            old_start = g.weak_start_date
            g.weak_start_date = today
            g.weak_days_count = 0
            log.info(f"🟡 【走弱期延续】再次触发进入条件，重置计数器")
        else:
            log.info(f"🔴 【走弱期中】已持续{g.weak_days_count}/{g.max_weak_days}个交易日")
    else:
        if weak_condition_met:
            log.info(f"🔴 【走弱期进入】触发进入条件，进入大A走弱期")
            g.is_a_share_weak = True
            g.weak_start_date = today
            g.weak_days_count = 0
        else:
            log.info(f"🟢 【正常期中】未满足进入条件")
    
    status_emoji = "🔴" if g.is_a_share_weak else "🟢"
    status_str = f"{status_emoji} 最终状态: 走弱期={g.is_a_share_weak}"
    if g.is_a_share_weak:
        status_str += f" (已持续{g.weak_days_count}/{g.max_weak_days}个交易日)"
        record(走弱期状态=1)
    else:
        record(走弱期状态=0)
    log.info(f"📊 【走弱期判断】{status_str}")
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


def get_final_ranked_etfs(context, quiet=False):
    """全流程动量排名→候选→结合持仓得最终目标。quiet=True 静默（预报告复用），计算逻辑与非 quiet 完全一致。"""
    all_metrics = []
    etf_set = list(g.merged_etf_pool)
    end_date = context.previous_date
    if not quiet:
        log.info(f"【动量得分计算】使用合并池，合计{len(etf_set)}只ETF")
        log.info(f"【当前状态】{'🔴 大A走弱期' if g.is_a_share_weak else '🟢 大A正常期'}")
    lookback = max(g.lookback_days, g.volume_lookback, g.ma_lookback) + 20
    today = context.current_dt.date()
    current_data = get_current_data()
    safe_lookback = lookback + 20
    hist_df = get_price(etf_set, count=safe_lookback, end_date=end_date, frequency='1d', fields=['close', 'volume'], panel=False)
    today_vol_df = get_price(etf_set, start_date=today, end_date=context.current_dt, frequency='1m', fields=['volume'], panel=False, fill_paused=False)
    if hist_df is None or hist_df.empty:
        log.warning("【动量计算】无法获取历史价格数据")
        return []
    today_vols = today_vol_df.groupby('code')['volume'].sum() if (today_vol_df is not None and not today_vol_df.empty) else pd.Series(dtype=float)
    close_pivot = hist_df.pivot(index='time', columns='code', values='close')
    volume_pivot = hist_df.pivot(index='time', columns='code', values='volume')
    # ========== 遍历ETF计算动量得分 ==========
    skipped_no_minute = []
    for etf in etf_set:
        try:
            if current_data[etf].paused:
                continue
            if is_temporarily_suspended(etf, context):
                log.info(f"⏭️ {etf} {get_security_name(etf)} 临时停牌/分钟数据缺失，跳过动量计算")
                continue
            if etf not in close_pivot.columns:
                continue
            raw_closes = close_pivot[etf].values
            raw_volumes = volume_pivot[etf].values
            valid_mask = (~np.isnan(raw_volumes)) & (raw_volumes > 0)
            hist_closes = raw_closes[valid_mask]
            hist_volumes = raw_volumes[valid_mask]
            hist_closes = hist_closes[-lookback:]
            hist_volumes = hist_volumes[-lookback:]
            if len(hist_closes) < g.lookback_days:
                continue
            etf_name = get_security_name(etf)
            current_price = current_data[etf].last_price
            today_vol = today_vols.get(etf, 0)
            metrics = calculate_all_metrics_for_etf(etf, etf_name, hist_closes, hist_volumes, current_price, today_vol, context)
        except RuntimeError as e:
            skipped_no_minute.append((etf, get_security_name(etf), str(e)))
            log.warning(f"⚠️ {etf} {get_security_name(etf)} 分钟数据获取失败，跳过: {e}")
            continue
        if metrics:
            if metrics['etf'] in {m['etf'] for m in all_metrics}:
                continue
            all_metrics.append(metrics)
    if skipped_no_minute:
        log.warning(f"⚠️ 共{len(skipped_no_minute)}只ETF因分钟数据缺失被跳过:")
        for code, name, reason in skipped_no_minute:
            log.warning(f"  - {code} {name}: {reason}")
    for item in all_metrics:
        score = item.get('momentum_score')
        if pd.isna(score) or (isinstance(score, float) and np.isnan(score)):
            item['momentum_score'] = float('-inf')
    # 按动量得分排序
    all_metrics.sort(key=lambda x: x.get('momentum_score', float('-inf')), reverse=True)
    # 记录今日实际完成评估的标的集合（供卖出流水线数据缺失保护兜底使用）
    g._assessed_codes = {m['etf'] for m in all_metrics}
    # ========== 第一步：输出所有ETF排序表格 ==========
    log_buffer = []
    log_buffer.append("")
    log_buffer.append(">>> 第一步：所有ETF按动量得分从大到小排序 <<<")
    for m in all_metrics[:100]:
        def fmt_status(value_str, passed):
            return f"{value_str} {'✅' if passed else '❌'}"
        score_str = f"{m['momentum_score']:.4f}" if m['momentum_score'] != float('-inf') else "nan"
        r2_str = f"{m['r_squared']:.3f}" if not pd.isna(m['r_squared']) else "nan"
        vol_val = f"{m['volume_ratio']:.2f}" if m['volume_ratio'] is not None else "N/A"
        min_ratio = min(m['day_ratios']) if m['day_ratios'] else 'N/A'
        loss_val = f"{min_ratio:.4f}" if isinstance(min_ratio, float) and not pd.isna(min_ratio) else str(min_ratio)
        ma_str = f"MA{g.ma_lookback}: {m['ma_value']:.2f}" if m['ma_value'] is not None else "MA:N/A"
        line = (
            f"{m['etf']} {m['etf_name']}: "
            f"动量得分: {fmt_status(score_str, m['passed_momentum'])}，"
            f"R²: {fmt_status(r2_str, m['passed_r2'])}，"
            f"均线: {fmt_status(ma_str, m['passed_ma'])}，"
            f"成交量比值: {fmt_status(vol_val, m['passed_volume'])}，"
            f"短期风控: {fmt_status(loss_val, m['passed_loss'])}"
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
                return f"{value_str} {'✅' if passed else '❌'}"
            score_str = f"{m['momentum_score']:.4f}" if m['momentum_score'] != float('-inf') else "nan"
            r2_str = f"{m['r_squared']:.3f}" if not pd.isna(m['r_squared']) else "nan"
            vol_val = f"{m['volume_ratio']:.2f}" if m['volume_ratio'] is not None else "N/A"
            min_ratio = min(m['day_ratios']) if m['day_ratios'] else 'N/A'
            loss_val = f"{min_ratio:.4f}" if isinstance(min_ratio, float) and not pd.isna(min_ratio) else str(min_ratio)
            ma_str = f"MA{g.ma_lookback}: {m['ma_value']:.2f}" if m['ma_value'] is not None else "MA:N/A"
            line = (
                f"{m['etf']} {m['etf_name']}: "
                f"动量得分: {fmt_status(score_str, m['passed_momentum'])}，"
                f"R²: {fmt_status(r2_str, m['passed_r2'])}，"
                f"均线: {fmt_status(ma_str, m['passed_ma'])}，"
                f"成交量比值: {fmt_status(vol_val, m['passed_volume'])}，"
                f"短期风控: {fmt_status(loss_val, m['passed_loss'])}"
            )
            log_buffer.append(line)
    else:
        log_buffer.append("（无符合条件的ETF）")
        full_log = "\n".join(log_buffer)
        if not quiet:
            log.info(full_log)
        return []
    # ========== 第三步：确定候选池 ==========
    score_key = 'momentum_score'
    if len(top_10) >= g.holdings_num:
        reference_score = top_10[g.holdings_num - 1].get(score_key, float('-inf'))
        ratio = g.score_threshold_ratio if not g.is_a_share_weak else 1.0
        score_threshold = reference_score * ratio
        log_buffer.append("")
        log_buffer.append(f">>> 第三步：选取动量得分≥第{g.holdings_num}名({top_10[g.holdings_num - 1]['etf_name']})得分{reference_score:.4f}×{g.score_threshold_ratio}={score_threshold:.4f}的ETF <<<")
        candidate_pool = [item for item in top_10 if item.get(score_key, float('-inf')) >= score_threshold]
    else:
        log_buffer.append("")
        log_buffer.append(f">>> 第三步：前10名不足{g.holdings_num}只，全部作为候选池 <<<")
        candidate_pool = top_10[:]
    log_buffer.append(f"【候选池】共{len(candidate_pool)}只ETF（按动量得分排序）：")
    for i, item in enumerate(candidate_pool):
        log_buffer.append(f"  {i+1}. {item['etf_name']}({item['etf']}) {score_key}: {item.get(score_key, 0):.4f}")
    # ========== 第四步：结合当前持仓进行调整 ==========
    log_buffer.append("")
    log_buffer.append(">>> 第四步：结合当前持仓进行调整 <<<")
    current_holdings = [sec for sec, pos in context.portfolio.positions.items() if pos.total_amount > 0]
    log_buffer.append(f"当前持仓ETF：{current_holdings}")
    candidate_dict = {item['etf']: item for item in candidate_pool}
    retained = [candidate_dict[etf] for etf in current_holdings if etf in candidate_dict]
    log_buffer.append(f"其中存在于候选池中的持仓ETF：{[item['etf'] for item in retained]}")
    # ========== 数据缺失保护（v5.4.1）：持仓未参与动量计算时保守保留 ==========
    # 取数瞬时失败曾让持仓从排名中静默消失并被无警告卖出（08-17~08-21 连续
    # 误换仓：教育/德国/黄金轮流"被失踪"，每次白付佣金且卖飞上涨标的）。
    # 持仓在合并池内却不在计算结果中 = 今日无法评估 → 保留持仓、告警并推钉钉，
    # 不基于残缺排名换仓。防御型ETF（如 511880）不在合并池内，不触发本保护。
    assessed_codes = {m['etf'] for m in all_metrics}
    pool_set = set(g.merged_etf_pool)
    retained_set0 = {r['etf'] for r in retained}
    for etf in current_holdings:
        if etf in assessed_codes or etf not in pool_set or etf in retained_set0:
            continue
        name = get_security_name(etf)
        if not quiet:
            log.warning(f"🛡️ 【数据缺失保护】{etf} {name} 在合并池内但未参与今日动量计算（日线/分钟取数失败），保守保留持仓不换仓")
            try:
                log.notify(f"🛡️ 数据缺失保护：{name}({etf}) 今日动量数据缺失，保留持仓不换仓")
            except Exception:
                pass
        retained.append({'etf': etf, 'etf_name': name, 'momentum_score': float('inf')})
    # ========== A2 持仓宽容（v5.3）：持仓未进候选池但得分仍高时保留 ==========
    hold_buffer = getattr(g, 'hold_buffer', 1.0)
    if hold_buffer < 1.0 and len(retained) < g.holdings_num:
        pool_threshold = score_threshold * hold_buffer if len(top_10) >= g.holdings_num else float('-inf')
        full_dict = {item['etf']: item for item in filtered_list}
        retained_set = {r['etf'] for r in retained}
        for etf in current_holdings:
            if etf in retained_set or etf not in full_dict:
                continue
            holding = full_dict[etf]
            holding_score = holding.get(score_key, float('-inf'))
            if holding_score != float('-inf') and holding_score >= pool_threshold:
                retained.append(holding)
                retained_set.add(etf)
                log_buffer.append(f"【持仓宽容】{holding['etf_name']}({etf}) 得分{holding_score:.4f} ≥ 候选池门槛×{hold_buffer}={pool_threshold:.4f}，保留持仓")
    if len(retained) >= g.holdings_num:
        retained_sorted = sorted(retained, key=lambda x: x.get(score_key, float('-inf')), reverse=True)
        final_result = retained_sorted[:g.holdings_num]
        log_buffer.append(f"保留的持仓ETF数量({len(retained)})超过目标持仓数({g.holdings_num})，将从保留的ETF中按动量得分取前{g.holdings_num}只作为最终目标。")
    else:
        need = g.holdings_num - len(retained)
        remaining_pool = [item for item in candidate_pool if item['etf'] not in {r['etf'] for r in retained}]
        additional = remaining_pool[:need]
        final_result = retained + additional
        log_buffer.append(f"保留持仓ETF {len(retained)}只，还需补充{need}只。")
        if retained:
            log_buffer.append("保留的ETF（按原有顺序）：")
            for item in retained:
                log_buffer.append(f"  {item['etf_name']}({item['etf']})")
        if additional:
            log_buffer.append("补充的ETF（按动量得分排序）：")
            for i, item in enumerate(additional):
                log_buffer.append(f"  {i+1}. {item['etf_name']}({item['etf']}) {score_key}: {item.get(score_key, 0):.4f}")
    log_buffer.append(f"【最终目标】共{len(final_result)}只ETF：")
    for i, item in enumerate(final_result):
        log_buffer.append(f"  {i+1}. {item['etf_name']}({item['etf']})")
    log_buffer.append("==================================================")
    full_log = "\n".join(log_buffer)
    if not quiet:
        log.info(full_log)
    return final_result


def execute_sell_trades(context):
    log.info("========== 卖出操作开始 ==========")
    ranked_etfs = getattr(g, 'ranked_etfs_result', [])
    target_etfs = []
    
    if ranked_etfs:
        for metrics in ranked_etfs[:g.holdings_num]:
            target_etfs.append(metrics['etf'])
            log.info(f"确定最终目标: {metrics['etf']} {metrics['etf_name']}")
    else:
        if check_defensive_etf_available(context):
            target_etfs = [g.defensive_etf]
            etf_name = get_security_name(g.defensive_etf)
            log.info(f"🛡️ 确定最终目标(防御模式): {g.defensive_etf} {etf_name}")
        else:
            log.info("💤 无最终目标(空仓模式)")
            target_etfs = []
    
    g.target_etfs_list = target_etfs
    current_positions = list(context.portfolio.positions.keys())
    target_set = set(target_etfs)
    sell_count = 0
    
    for security in current_positions:
        position = context.portfolio.positions[security]
        if position.total_amount > 0 and security not in target_set:
            # 数据缺失保护兜底：排名整体为空（hist_df 取数失败早退）走防御模式时，
            # 在池内但未参与评估的持仓不卖——宁可少动，不可基于残缺数据清仓。
            assessed = getattr(g, '_assessed_codes', None)
            if (assessed is not None
                    and security in set(getattr(g, 'merged_etf_pool', []) or [])
                    and security not in assessed):
                security_name = get_security_name(security)
                log.warning(f"🛡️ 【数据缺失保护】{security} {security_name} 未参与今日动量计算，跳过卖出（保守保留）")
                try:
                    log.notify(f"🛡️ 数据缺失保护：{get_security_name(security)}({security}) 今日动量数据缺失，跳过卖出")
                except Exception:
                    pass
                continue
            security_name = get_security_name(security)
            success = smart_order_target_value(security, 0, context)
            if success:
                sell_count += 1
                log.info(f"✅ 已成功卖出: {security} {security_name}")
    
    log.info(f"本次共计划卖出{sell_count}只ETF。")
    log.info("========== 卖出操作完成 ==========")


def execute_buy_trades(context):
    log.info("========== 买入操作开始 ==========")
    target_etfs = g.target_etfs_list
    
    if not target_etfs:
        log.info("根据计算的结果，今日无目标ETF，保持空仓")
        log.info("========== 买入操作完成 ==========")
        return
    
    current_positions = set(context.portfolio.positions.keys())
    etfs_to_buy = [etf for etf in target_etfs if etf not in current_positions]
    actual_holding_count = len(current_positions)
    max_buy_count = max(0, g.holdings_num - actual_holding_count)
    num_etfs_to_buy = min(len(etfs_to_buy), max_buy_count)
    
    if num_etfs_to_buy <= 0:
        log.info(f"当前实际持仓数量({actual_holding_count})已达到或超过目标({g.holdings_num})，无需买入")
        log.info("========== 买入操作完成 ==========")
        return
    
    etfs_to_buy = etfs_to_buy[:num_etfs_to_buy]
    log.info(f"当前实际持仓: {actual_holding_count}只, 目标持仓: {g.holdings_num}只, 本次计划买入: {num_etfs_to_buy}只")

    # 完整过滤后排名（首选目标买不进时顺延下一名，避免空仓）
    ranked_full = getattr(g, 'ranked_candidates_full', []) or []
    fallback_order = [m['etf'] for m in ranked_full]
    bought_etfs = set(current_positions)  # 已持有/已买入的不再重复买

    # 修复：动态分配资金，避免可用现金为负
    for i in range(num_etfs_to_buy):
        remaining_cash = context.portfolio.available_cash
        if remaining_cash < g.min_money:
            log.info(f"可用现金 {remaining_cash:.2f} 不足最小交易额 {g.min_money:.2f}，停止买入")
            break

        remaining_to_buy = num_etfs_to_buy - i
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
            log.info(f"为 {cand} 分配目标金额: {target_value_for_this_etf:.2f} 元 (剩余现金 {remaining_cash:.2f}, 待买数量 {remaining_to_buy})")
            if smart_order_target_value(cand, target_value_for_this_etf, context):
                log.info(f"✅ ETF {cand} 下单成功")
                bought_etfs.add(cand)
                success = True
                break
            else:
                log.info(f"⚠️ {cand} 买入失败(涨停/停牌等)，顺延下一名候选")
        if not success:
            log.info(f"❌ 本轮无可用候选ETF可买入(均涨停/停牌)，停止买入")

    log.info("========== 买入操作完成 ==========")


def is_temporarily_suspended(security, context, minute_count=10):
    """
    判断ETF是否盘中临时停牌
    通过检查最近N分钟是否有成交来判断，若无成交则视为临时停牌
    """
    try:
        # 获取最近N分钟的分钟线数据
        minute_data = get_price(
            security,
            end_date=context.current_dt,
            count=minute_count,
            frequency='1m',
            fields=['volume'],
            skip_paused=False,
            fq='pre'
        )
        # 无数据或数据为空：可能是真实停牌，也可能是当日分钟数据缺失
        # （实时盘中当日分区未落盘/回源失败）。两者后果不同——数据缺失把活跃标的
        # 误判成停牌会静默踢出动量排名（08-12 159768 案例）。区分并记录，便于排查。
        if minute_data is None or minute_data.empty:
            log.warning(f"⚠️ {security} {get_security_name(security)} 分钟数据缺失"
                        f"（{context.current_dt} 最近{minute_count}分钟无数据），"
                        f"按临时停牌处理，若为盘中取数异常请排查数据源")
            return True
        # 最近N分钟成交量都为0，视为临时停牌
        if (minute_data['volume'] == 0).all():
            log.info(f"🔇 {security} {get_security_name(security)} 最近{minute_count}分钟无成交，判定临时停牌")
            return True
        return False
    except Exception as e:
        log.debug(f"临时停牌检测异常 {security}: {e}")
        return False  # 异常时默认认为正常，避免误判


def smart_order_target_value(security, target_value, context):
    """
    智能下单：根据目标市值调整持仓，处理停牌、涨跌停、最小交易金额、T+1
    """
    data = get_current_data()
    name = get_security_name(security)
    # ========== 1. 全天停牌检测 ==========
    if data[security].paused:
        log.info(f"{security} {name} 全天停牌，跳过交易")
        return False
    # ========== 2. 盘中临时停牌检测 ==========
    if is_temporarily_suspended(security, context):
        log.info(f"{security} {name} 盘中临时停牌，跳过交易")
        return False
    price = data[security].last_price
    if price == 0:
        log.info(f"{security} {name} 当前价格为0，跳过交易")
        return False
    # ========== 3. 买入时使用预估成交价（包含佣金+滑点）计算股数 ==========
    if target_value > 0:
        buy_commission_rate = 0.0001   # 买入佣金
        slippage_rate = 0.0001         # 滑点
        estimated_price = price * (1 + buy_commission_rate + slippage_rate)
        target_amount = int(target_value / estimated_price)
        target_amount = (target_amount // 100) * 100
        if target_amount <= 0:
            target_amount = 100
        # 二次校验：用实时可用现金和预估成交价(含佣金+滑点)严格限制（兜底），
        # 必须用 estimated_price 而非 price，否则股数对应的真实成本(含费)会略微
        # 超过可用现金而被 order() 拒绝，导致本应成交的买入被整笔跳过、序列错位。
        max_shares = int(context.portfolio.available_cash / estimated_price)
        max_shares = (max_shares // 100) * 100
        if max_shares < target_amount:
            target_amount = max_shares
        if target_amount <= 0:
            log.info(f"{security} {name}: 现金不足买100股，跳过")
            return False
    else:
        target_amount = 0
    cur_pos = context.portfolio.positions.get(security, None)
    cur_amount = cur_pos.total_amount if cur_pos else 0
    diff = target_amount - cur_amount
    # ========== 4. 涨跌停检测（统一：涨停跌停都不交易） ==========
    if data[security].last_price >= data[security].high_limit:
        log.info(f"{security} {name} 当前涨停，跳过交易")
        return False
    if data[security].last_price <= data[security].low_limit:
        log.info(f"{security} {name} 当前跌停，跳过交易")
        return False
    trade_val = abs(diff) * price
    if 0 < trade_val < g.min_money:
        log.info(f"{security} {name} 交易金额{trade_val:.2f} < {g.min_money}，跳过")
        return False
    # ========== 5. T+1检查（仅卖出时） ==========
    if diff < 0:
        closeable = cur_pos.closeable_amount if cur_pos else 0
        if closeable == 0:
            log.info(f"{security} {name} 当天买入不可卖出(T+1)")
            return False
        diff = -min(abs(diff), closeable)
    # ========== 6. 执行下单 ==========
    if diff != 0:
        avg_cost = cur_pos.avg_cost if cur_pos else 0.0
        order_result = order(security, diff)
        if order_result:
            g._daily_traded = True                       # ding：当日已有下单，不再发「无换仓」通知
            if diff > 0:
                g._entry_date[security] = context.current_dt.date()
                log.info(f"📥 买入 {security} {name} 数量{abs(diff)} 价格{price:.3f} (预估含成本价: {estimated_price:.3f})")
                _notify_trade(security, name, "买入", abs(diff), price, avg_cost, context)
            else:
                log.info(f"📤 卖出 {security} {name} 数量{abs(diff)} 价格{price:.3f}")
                _notify_trade(security, name, "卖出", abs(diff), price, avg_cost, context)
                if cur_amount - abs(diff) <= 0:
                    g._entry_date.pop(security, None)
            return True
        else:
            log.warning(f"下单失败: {security} {name}，数量{diff}")
            return False
    return False


def _holding_trade_days(security, context):
    """持仓交易日数：买入日(含)到卖出日(不含)之间。取不到返回 None。"""
    entry = g._entry_date.get(security)
    if not entry:
        return None
    today = context.current_dt.date()
    if today <= entry:
        return 0
    try:
        days = get_trade_days(start_date=entry, end_date=today)
        return max(0, len(days) - 1)          # 减去卖出当日
    except Exception:
        return max(0, (today - entry).days)


def _notify_trade(security, name, action, amount, price, avg_cost, context):
    """买入/卖出钉钉通知。卖出时带盈亏与持仓天数，失败降级为 log.info。"""
    try:
        commission = abs(amount) * price * 0.0001
        if action == "卖出" and avg_cost > 0:
            pnl = (price - avg_cost) * amount
            pnl_pct = price / avg_cost - 1
            days = _holding_trade_days(security, context)
            days_str = f"{days}个交易日" if days is not None else "?天"
            log.notify(f"📤 卖出 {name}({security}) 数量{int(amount)} 价格{price:.3f} "
                       f"佣金{commission:.2f} 盈利{pnl:+.2f}({pnl_pct:+.2%}) 持仓{days_str}")
        else:
            log.notify(f"📥 买入 {name}({security}) 数量{int(amount)} 价格{price:.3f} "
                       f"佣金{commission:.2f}")
    except Exception:
        log.info(f"[notify] 成交通知组装失败 {security} {action}")


def minute_level_stop_loss(context):
    if not g.use_fixed_stop_loss:
        return
    current_time = context.current_dt.strftime('%H:%M')
    if not (('09:40' < current_time < '10:29') or ('10:40' < current_time < '11:30') or ('13:00' < current_time < '14:57')):
        return
    current_data = get_current_data()
    for security in list(context.portfolio.positions.keys()):
        position = context.portfolio.positions[security]
        if position.total_amount <= 0 or position.closeable_amount <= 0:
            if security in g._profit_protected:
                del g._profit_protected[security]
            if security in g._peak_price:
                del g._peak_price[security]
            continue
        current_price = current_data[security].last_price
        if current_price <= 0:
            continue
        cost_price = position.avg_cost
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
                log.info(f"🎯 【高位回落止盈】{security} {security_name} 从峰值{peak:.3f}回落至{current_price:.3f}，锁定盈利 {profit_ratio*100:.2f}%")
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
            stop_label = f"成本×{stop_threshold}" if stop_threshold >= 1.0 else f"成本×{g.fixedStopLossThreshold:.0%}"
            log.info(f"🚨 【分钟级固定止损】{security} {security_name} 触发止损({stop_label})，亏损: {loss_percent:.2f}%")
            smart_order_target_value(security, 0, context)


def get_security_name(security):
    try:
        if hasattr(g, 'etf_names_dict') and security in g.etf_names_dict:
            return g.etf_names_dict[security]
        return get_security_info(security).display_name
    except Exception:
        return "未知名称"


def check_defensive_etf_available(context):
    current_data = get_current_data()
    defensive_etf = g.defensive_etf
    if current_data[defensive_etf].paused:
        log.info(f"防御性ETF {defensive_etf} 今日停牌")
        return False
    if current_data[defensive_etf].last_price >= current_data[defensive_etf].high_limit:
        log.info(f"防御性ETF {defensive_etf} 当前涨停")
        return False
    if current_data[defensive_etf].last_price <= current_data[defensive_etf].low_limit:
        log.info(f"防御性ETF {defensive_etf} 当前跌停")
        return False
    return True


def trade(context):
    pass