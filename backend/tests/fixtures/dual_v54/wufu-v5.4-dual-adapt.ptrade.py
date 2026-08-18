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
# 平台差异适配说明（聚宽 → PTrade / 国金版本，按国金 PTrade 官方 API 文档对齐）：
#   - 代码格式：.SS / .SZ（策略内直接用 PTrade 码，无转换函数）
#   - 全局状态 g.* ：PTrade 同样支持并自动持久化
#   - 调度：晨间 → before_trading_start(context, data)；盘中 09:40/13:10/13:10/13:10 → run_daily(context, func, time)；
#           收盘 → after_trading_end(context, data)；分钟止损 → handle_data(context, data)
#   - 日线历史：get_history(count, '1d', field, security_list, fq='pre')；官方返回格式：
#             单标的（str security_list）列=行情字段（如 df['close']）；多标的（list）py3.11 长表（含 code 列）/
#             py3.5 宽表（代码列）。策略用 _series_values/_series_last（单标的）与 _wide（多标的）归一化。
#   - 盘中数据：PTrade 无 get_current_data()，由 handle_data 的 data 参数捕获到 _BARS；
#             data[code] 为 BarData 对象，含 open/high/low/close/price/volume/money/preclose/high_limit/low_limit；
#             停牌用 get_stock_status(stocks, query_type='HALT', query_date='YYYYmmdd')（_is_halted），
#             涨跌停价用日线 high_limit/low_limit 字段（_limit_prices）
#   - 持仓：get_position(sec) 返回 Position（amount / enable_amount / cost_basis / last_sale_price）；
#           get_positions() 返回 dict{code: Position}，键可能为 .XSHG/.XSHE（官方文档确认），
#           策略用 _positions() 归一为 .SS/.SZ 再与池子代码比较
#   - 现金/总资产：context.portfolio.cash / .portfolio_value（PTrade 无 get_cash）
#   - 动态 ETF 池：get_market_list()/get_market_detail() 枚举全市场基金（官方仅限 before_trading_start/after_trading_end 内调用，
#     _MARKET_ENUM_OK 守卫）；真机 get_market_detail 返回全市场产品，_get_all_fund_codes 按基金代码段
#     （SH 5xxxxx / SZ 15/16xxxx）过滤；取不到时优雅降级为固定池；成交额查询按 200 只分块
#     （_get_money_avg_series，money_corrected 真机无此字段自动回退官方 money）
#   - 日志：log.debug/info/warning/error/critical（无 log.warn）；set_benchmark/set_commission/set_slippage 仅回测可用，
#     真 PTrade 交易时 try/except 静默跳过
#   - record()/log.set_level/set_option 等聚宽独有 API 已移除
# ============================================================

import numpy as np
import math
import pandas as pd
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")


# ==================== 内嵌全市场 ETF 清单 ====================
# 由本地引擎 get_etf_list 生成（1660 只，纯 ETF），真机回测 get_etf_list 不可用、
# get_market_detail 枚举不可靠时兜底，保证真机与本地同口径。
_SHIPPED_ETF_CODES = [
    "159186.SZ", "515040.SS", "159527.SZ", "159279.SZ", "159085.SZ", "515880.SS", "562080.SS",
    "159996.SZ", "159351.SZ", "561170.SS", "159688.SZ", "515630.SS", "516460.SS", "560510.SS",
    "516390.SS", "589360.SS", "520670.SS", "159539.SZ", "159175.SZ", "516180.SS", "513210.SS",
    "159840.SZ", "506006.SS", "516700.SS", "561310.SS", "159291.SZ", "159827.SZ", "560910.SS",
    "159525.SZ", "513580.SS", "517080.SS", "516210.SS", "159269.SZ", "159229.SZ", "159245.SZ",
    "512560.SS", "511900.SS", "159505.SZ", "159703.SZ", "159837.SZ", "159613.SZ", "588140.SS",
    "159137.SZ", "563680.SS", "513320.SS", "159700.SZ", "516590.SS", "516950.SS", "515080.SS",
    "159848.SZ", "560190.SS", "510770.SS", "560630.SS", "159510.SZ", "159300.SZ", "516750.SS",
    "159145.SZ", "159964.SZ", "159307.SZ", "159041.SZ", "516220.SS", "589720.SS", "180201.SZ",
    "512100.SS", "159617.SZ", "526010.SS", "515210.SS", "506003.SS", "159038.SZ", "159992.SZ",
    "517030.SS", "517550.SS", "588860.SS", "159316.SZ", "159732.SZ", "159834.SZ", "512460.SS",
    "159546.SZ", "513220.SS", "159892.SZ", "589280.SS", "511380.SS", "516380.SS", "159520.SZ",
    "169106.SZ", "159218.SZ", "515260.SS", "159538.SZ", "159513.SZ", "561050.SS", "159242.SZ",
    "589060.SS", "159940.SZ", "159109.SZ", "159923.SZ", "159152.SZ", "562860.SS", "159210.SZ",
    "563700.SS", "520600.SS", "516630.SS", "517390.SS", "159057.SZ", "530000.SS", "530050.SS",
    "159153.SZ", "159379.SZ", "515190.SS", "560060.SS", "159243.SZ", "516270.SS", "589880.SS",
    "560090.SS", "159012.SZ", "520590.SS", "510810.SS", "517950.SS", "159543.SZ", "589560.SS",
    "159256.SZ", "518860.SS", "513970.SS", "588200.SS", "159627.SZ", "159337.SZ", "530680.SS",
    "563510.SS", "159690.SZ", "511260.SS", "516780.SS", "159887.SZ", "159786.SZ", "513890.SS",
    "159286.SZ", "159312.SZ", "510040.SS", "589770.SS", "560120.SS", "510380.SS", "510900.SS",
    "551580.SS", "180102.SZ", "561800.SS", "588100.SS", "562300.SS", "159738.SZ", "159666.SZ",
    "513050.SS", "560180.SS", "159532.SZ", "515400.SS", "589980.SS", "517520.SS", "513920.SS",
    "159298.SZ", "588240.SS", "560420.SS", "515970.SS", "159831.SZ", "512820.SS", "530530.SS",
    "561600.SS", "159812.SZ", "510210.SS", "180606.SZ", "159377.SZ", "159798.SZ", "520910.SS",
    "560110.SS", "159816.SZ", "516100.SS", "517000.SS", "159849.SZ", "159628.SZ", "560330.SS",
    "511010.SS", "159775.SZ", "520840.SS", "511620.SS", "517880.SS", "159285.SZ", "159272.SZ",
    "159158.SZ", "159960.SZ", "516830.SS", "560680.SS", "159237.SZ", "159079.SZ", "563890.SS",
    "159400.SZ", "159008.SZ", "588210.SS", "520620.SS", "560830.SS", "511970.SS", "159102.SZ",
    "159190.SZ", "588060.SS", "159157.SZ", "159160.SZ", "159925.SZ", "551030.SS", "563090.SS",
    "589150.SS", "159559.SZ", "510300.SS", "159570.SZ", "520650.SS", "159026.SZ", "159561.SZ",
    "561660.SS", "563800.SS", "512580.SS", "159306.SZ", "159336.SZ", "515730.SS", "159588.SZ",
    "159995.SZ", "589250.SS", "512680.SS", "513560.SS", "159596.SZ", "589010.SS", "526070.SS",
    "513520.SS", "562000.SS", "159758.SZ", "512140.SS", "513550.SS", "561560.SS", "510550.SS",
    "159531.SZ", "562910.SS", "516320.SS", "517660.SS", "513200.SS", "159518.SZ", "520630.SS",
    "512980.SS", "159590.SZ", "159398.SZ", "561020.SS", "515180.SS", "159326.SZ", "512020.SS",
    "560710.SS", "159830.SZ", "588790.SS", "513300.SS", "159891.SZ", "159822.SZ", "517300.SS",
    "159616.SZ", "159189.SZ", "180605.SZ", "159030.SZ", "159068.SZ", "159991.SZ", "159325.SZ",
    "588500.SS", "159138.SZ", "159701.SZ", "159935.SZ", "588680.SS", "513700.SS", "159039.SZ",
    "159748.SZ", "511850.SS", "589210.SS", "159537.SZ", "512650.SS", "159251.SZ", "563180.SS",
    "563300.SS", "180106.SZ", "516760.SS", "561770.SS", "159670.SZ", "510390.SS", "159263.SZ",
    "520760.SS", "515700.SS", "159355.SZ", "562990.SS", "506005.SS", "159770.SZ", "159695.SZ",
    "588840.SS", "589190.SS", "159937.SZ", "159916.SZ", "561250.SS", "159825.SZ", "159315.SZ",
    "159016.SZ", "159845.SZ", "562340.SS", "159607.SZ", "159035.SZ", "516230.SS", "159856.SZ",
    "516650.SS", "517170.SS", "513950.SS", "159621.SZ", "159720.SZ", "159615.SZ", "180302.SZ",
    "560260.SS", "510330.SS", "563380.SS", "159660.SZ", "517110.SS", "563760.SS", "159869.SZ",
    "159847.SZ", "159120.SZ", "159797.SZ", "515250.SS", "563560.SS", "512890.SS", "510160.SS",
    "515450.SS", "513660.SS", "516800.SS", "561160.SS", "513630.SS", "159576.SZ", "589950.SS",
    "510030.SS", "159776.SZ", "560900.SS", "516300.SS", "159938.SZ", "159620.SZ", "588370.SS",
    "562520.SS", "563050.SS", "159155.SZ", "562880.SS", "560580.SS", "159385.SZ", "510880.SS",
    "560470.SS", "159048.SZ", "159193.SZ", "159387.SZ", "159905.SZ", "510230.SS", "159673.SZ",
    "159782.SZ", "180607.SZ", "159736.SZ", "159009.SZ", "588810.SS", "562570.SS", "159728.SZ",
    "180603.SZ", "159773.SZ", "563580.SS", "159741.SZ", "515070.SS", "563850.SS", "512180.SS",
    "159571.SZ", "588000.SS", "159021.SZ", "159127.SZ", "159555.SZ", "561500.SS", "520550.SS",
    "180202.SZ", "561180.SS", "520580.SS", "588190.SS", "159335.SZ", "159902.SZ", "588940.SS",
    "159258.SZ", "563150.SS", "560310.SS", "159781.SZ", "588910.SS", "551300.SS", "159369.SZ",
    "588160.SS", "159783.SZ", "159106.SZ", "159388.SZ", "563390.SS", "159276.SZ", "561000.SS",
    "159671.SZ", "510190.SS", "562890.SS", "159865.SZ", "180101.SZ", "515750.SS", "516920.SS",
    "510270.SS", "159933.SZ", "589020.SS", "588310.SS", "159945.SZ", "516980.SS", "159597.SZ",
    "159943.SZ", "159793.SZ", "159852.SZ", "516120.SS", "513070.SS", "159682.SZ", "159360.SZ",
    "159113.SZ", "159556.SZ", "588390.SS", "159389.SZ", "560450.SS", "159075.SZ", "159167.SZ",
    "516890.SS", "560500.SS", "561910.SS", "180301.SZ", "516880.SS", "520730.SS", "513380.SS",
    "511830.SS", "159697.SZ", "159751.SZ", "159975.SZ", "159500.SZ", "563200.SS", "516770.SS",
    "516970.SS", "510360.SS", "560950.SS", "588750.SS", "159328.SZ", "511670.SS", "589110.SS",
    "159015.SZ", "159023.SZ", "180501.SZ", "159091.SZ", "563690.SS", "588760.SS", "159899.SZ",
    "588820.SS", "159723.SZ", "159928.SZ", "511800.SS", "159906.SZ", "512430.SS", "561460.SS",
    "560030.SS", "159107.SZ", "563280.SS", "159222.SZ", "562500.SS", "159752.SZ", "516260.SS",
    "561550.SS", "515710.SS", "515170.SS", "159142.SZ", "159507.SZ", "159954.SZ", "159136.SZ",
    "560270.SS", "159547.SZ", "511220.SS", "159383.SZ", "561320.SS", "563620.SS", "159399.SZ",
    "512360.SS", "159855.SZ", "159311.SZ", "515350.SS", "159881.SZ", "560100.SS", "517400.SS",
    "588110.SS", "159939.SZ", "589330.SS", "588980.SS", "563330.SS", "159122.SZ", "511980.SS",
    "516090.SS", "551500.SS", "520960.SS", "561080.SS", "159045.SZ", "512570.SS", "516520.SS",
    "511100.SS", "560480.SS", "513780.SS", "510020.SS", "159163.SZ", "159359.SZ", "512520.SS",
    "159967.SZ", "159977.SZ", "159957.SZ", "515920.SS", "588010.SS", "588020.SS", "159103.SZ",
    "159168.SZ", "560920.SS", "159850.SZ", "159378.SZ", "510010.SS", "561630.SS", "159630.SZ",
    "530180.SS", "563880.SS", "520820.SS", "159948.SZ", "159037.SZ", "159288.SZ", "560390.SS",
    "159699.SZ", "511700.SS", "518600.SS", "563600.SS", "159530.SZ", "159110.SZ", "159814.SZ",
    "159148.SZ", "560720.SS", "560660.SS", "159707.SZ", "515590.SS", "516960.SS", "588030.SS",
    "159135.SZ", "159295.SZ", "159373.SZ", "159031.SZ", "159201.SZ", "159213.SZ", "159033.SZ",
    "159638.SZ", "159563.SZ", "159297.SZ", "512600.SS", "159287.SZ", "159197.SZ", "517350.SS",
    "159610.SZ", "159982.SZ", "159971.SZ", "562580.SS", "159550.SZ", "159185.SZ", "511930.SS",
    "510180.SS", "159221.SZ", "159976.SZ", "159066.SZ", "512120.SS", "159027.SZ", "159056.SZ",
    "159019.SZ", "512760.SS", "159600.SZ", "159711.SZ", "530080.SS", "526030.SS", "159609.SZ",
    "169101.SZ", "515120.SS", "513350.SS", "588880.SS", "159871.SZ", "159640.SZ", "159032.SZ",
    "515360.SS", "159795.SZ", "513730.SS", "510660.SS", "159876.SZ", "517990.SS", "159768.SZ",
    "159195.SZ", "159883.SZ", "159268.SZ", "159018.SZ", "563570.SS", "512380.SS", "159509.SZ",
    "159239.SZ", "515760.SS", "159206.SZ", "159257.SZ", "159366.SZ", "159566.SZ", "159599.SZ",
    "589100.SS", "560850.SS", "159667.SZ", "510570.SS", "511030.SS", "159338.SZ", "513040.SS",
    "520930.SS", "561750.SS", "561510.SS", "560790.SS", "159811.SZ", "512690.SS", "589550.SS",
    "515380.SS", "159130.SZ", "159028.SZ", "159908.SZ", "159233.SZ", "159266.SZ", "159663.SZ",
    "159231.SZ", "513720.SS", "589400.SS", "159230.SZ", "159552.SZ", "159601.SZ", "159872.SZ",
    "520520.SS", "520870.SS", "159763.SZ", "516560.SS", "510410.SS", "515890.SS", "561200.SS",
    "510050.SS", "159922.SZ", "159202.SZ", "551560.SS", "589320.SS", "512720.SS", "159303.SZ",
    "560730.SS", "159361.SZ", "159536.SZ", "159072.SZ", "588890.SS", "159247.SZ", "589520.SS",
    "561370.SS", "561470.SS", "588470.SS", "510950.SS", "561450.SS", "159051.SZ", "159806.SZ",
    "159743.SZ", "515220.SS", "560690.SS", "589220.SS", "561700.SS", "159649.SZ", "513110.SS",
    "159362.SZ", "159192.SZ", "516670.SS", "159972.SZ", "159652.SZ", "159820.SZ", "561090.SS",
    "511160.SS", "560550.SS", "588990.SS", "159169.SZ", "520830.SS", "530880.SS", "510910.SS",
    "159178.SZ", "511060.SS", "159593.SZ", "159796.SZ", "530100.SS", "159685.SZ", "515790.SS",
    "588330.SS", "159177.SZ", "562960.SS", "159912.SZ", "563550.SS", "560290.SS", "159800.SZ",
    "563320.SS", "159249.SZ", "560410.SS", "513900.SS", "515050.SS", "159372.SZ", "588400.SS",
    "159761.SZ", "589500.SS", "520850.SS", "159665.SZ", "560170.SS", "562930.SS", "159718.SZ",
    "516930.SS", "159755.SZ", "511130.SS", "511600.SS", "511910.SS", "159777.SZ", "562560.SS",
    "511770.SS", "159099.SZ", "515110.SS", "588480.SS", "159553.SZ", "517130.SS", "518800.SS",
    "520890.SS", "159139.SZ", "516570.SS", "516350.SS", "159679.SZ", "561760.SS", "589160.SS",
    "512960.SS", "159332.SZ", "513930.SS", "159036.SZ", "159076.SZ", "159787.SZ", "159065.SZ",
    "561280.SS", "159745.SZ", "562310.SS", "512930.SS", "515950.SS", "159591.SZ", "562050.SS",
    "561920.SS", "516510.SS", "512970.SS", "562900.SS", "159896.SZ", "560990.SS", "159549.SZ",
    "560930.SS", "512880.SS", "588830.SS", "159501.SZ", "159516.SZ", "510200.SS", "159560.SZ",
    "561880.SS", "159567.SZ", "159108.SZ", "563060.SS", "588130.SS", "516790.SS", "563860.SS",
    "515090.SS", "159367.SZ", "512390.SS", "513130.SS", "159980.SZ", "159698.SZ", "513820.SS",
    "516370.SS", "159898.SZ", "511960.SS", "159838.SZ", "159944.SZ", "561990.SS", "589630.SS",
    "159150.SZ", "159117.SZ", "159232.SZ", "159965.SZ", "159903.SZ", "562820.SS", "589420.SS",
    "563220.SS", "159778.SZ", "560820.SS", "159241.SZ", "511860.SS", "159623.SZ", "512870.SS",
    "560570.SS", "560370.SS", "159180.SZ", "159212.SZ", "159327.SZ", "159675.SZ", "516580.SS",
    "159828.SZ", "563630.SS", "159386.SZ", "513770.SS", "561190.SS", "589680.SS", "510350.SS",
    "513060.SS", "159735.SZ", "510500.SS", "512240.SS", "526000.SS", "510850.SS", "589660.SS",
    "510100.SS", "159637.SZ", "159156.SZ", "551520.SS", "512220.SS", "563830.SS", "530800.SS",
    "512190.SS", "159370.SZ", "159713.SZ", "159368.SZ", "562530.SS", "513980.SS", "515230.SS",
    "159395.SZ", "159260.SZ", "588530.SS", "159595.SZ", "159042.SZ", "159573.SZ", "516610.SS",
    "159281.SZ", "560460.SS", "159205.SZ", "159645.SZ", "159747.SZ", "159842.SZ", "159280.SZ",
    "589120.SS", "562380.SS", "562590.SS", "510060.SS", "159582.SZ", "560670.SS", "180601.SZ",
    "159851.SZ", "511650.SS", "159390.SZ", "180602.SZ", "159889.SZ", "588120.SS", "159647.SZ",
    "517380.SS", "561870.SS", "159790.SZ", "589780.SS", "520660.SS", "513600.SS", "562060.SS",
    "159087.SZ", "159179.SZ", "159209.SZ", "161226.SZ", "159188.SZ", "159581.SZ", "516130.SS",
    "515160.SS", "589410.SS", "159729.SZ", "159119.SZ", "159791.SZ", "510710.SS", "513990.SS",
    "159172.SZ", "159792.SZ", "516730.SS", "515720.SS", "560490.SS", "159393.SZ", "588870.SS",
    "180502.SZ", "589390.SS", "513100.SS", "512060.SS", "588220.SS", "159726.SZ", "510670.SS",
    "159835.SZ", "516640.SS", "159602.SZ", "159121.SZ", "510140.SS", "159352.SZ", "588090.SS",
    "563990.SS", "520780.SS", "159293.SZ", "159657.SZ", "520530.SS", "517100.SS", "560800.SS",
    "159061.SZ", "512910.SS", "159913.SZ", "589310.SS", "512090.SS", "159271.SZ", "512510.SS",
    "516310.SS", "159568.SZ", "560380.SS", "515150.SS", "159170.SZ", "159011.SZ", "561580.SS",
    "511520.SS", "159541.SZ", "159132.SZ", "159265.SZ", "510510.SS", "515850.SS", "561230.SS",
    "560010.SS", "512040.SS", "159296.SZ", "563790.SS", "563980.SS", "515390.SS", "159323.SZ",
    "159629.SZ", "516660.SS", "159517.SZ", "159318.SZ", "515560.SS", "562920.SS", "561490.SS",
    "159196.SZ", "520710.SS", "159952.SZ", "513010.SS", "159716.SZ", "159353.SZ", "513750.SS",
    "513650.SS", "562700.SS", "512450.SS", "561420.SS", "159959.SZ", "159895.SZ", "510130.SS",
    "159934.SZ", "589290.SS", "563770.SS", "159642.SZ", "159528.SZ", "561120.SS", "515100.SS",
    "516620.SS", "159930.SZ", "588950.SS", "159053.SZ", "506001.SS", "159558.SZ", "560250.SS",
    "159540.SZ", "159261.SZ", "159958.SZ", "588670.SS", "512150.SS", "159105.SZ", "516080.SS",
    "159739.SZ", "512940.SS", "159350.SZ", "516810.SS", "511070.SS", "589890.SS", "159255.SZ",
    "159712.SZ", "513000.SS", "513030.SS", "530580.SS", "159125.SZ", "516710.SS", "159689.SZ",
    "159396.SZ", "512080.SS", "180701.SZ", "513620.SS", "159273.SZ", "159365.SZ", "510630.SS",
    "512410.SS", "516530.SS", "159049.SZ", "513290.SS", "513330.SS", "511020.SS", "159810.SZ",
    "512770.SS", "561430.SS", "517090.SS", "506002.SS", "562030.SS", "513500.SS", "588420.SS",
    "159941.SZ", "562510.SS", "520720.SS", "520950.SS", "159719.SZ", "159618.SZ", "159717.SZ",
    "562320.SS", "159635.SZ", "159760.SZ", "516500.SS", "512030.SS", "563590.SS", "180203.SZ",
    "159966.SZ", "515640.SS", "159022.SZ", "515010.SS", "589050.SS", "159731.SZ", "588350.SS",
    "516900.SS", "159040.SZ", "517200.SS", "589990.SS", "511120.SS", "159873.SZ", "517770.SS",
    "560880.SS", "516360.SS", "159181.SZ", "159973.SZ", "588800.SS", "159655.SZ", "159696.SZ",
    "589860.SS", "589180.SS", "159059.SZ", "159208.SZ", "159187.SZ", "520770.SS", "159780.SZ",
    "563230.SS", "510650.SS", "159686.SZ", "159866.SZ", "159029.SZ", "180402.SZ", "517900.SS",
    "159994.SZ", "515770.SS", "588700.SS", "159227.SZ", "159101.SZ", "180305.SZ", "588450.SS",
    "515020.SS", "513120.SS", "520790.SS", "159901.SZ", "516200.SS", "562800.SS", "560700.SS",
    "589230.SS", "159920.SZ", "159633.SZ", "513310.SS", "159321.SZ", "159653.SZ", "516860.SS",
    "159371.SZ", "159248.SZ", "512810.SS", "159721.SZ", "588300.SS", "159750.SZ", "159111.SZ",
    "517180.SS", "159267.SZ", "159565.SZ", "561330.SS", "561960.SS", "159100.SZ", "512630.SS",
    "180105.SZ", "589850.SS", "589820.SS", "159001.SZ", "513850.SS", "589900.SS", "159824.SZ",
    "513800.SS", "159302.SZ", "159931.SZ", "563350.SS", "511580.SS", "588710.SS", "512530.SS",
    "561010.SS", "589600.SS", "562360.SS", "516000.SS", "562600.SS", "530300.SS", "159240.SZ",
    "560230.SS", "159067.SZ", "561400.SS", "561300.SS", "560080.SS", "510600.SS", "159123.SZ",
    "511150.SS", "159356.SZ", "159813.SZ", "159656.SZ", "159339.SZ", "515310.SS", "513150.SS",
    "562350.SS", "561980.SS", "159199.SZ", "560810.SS", "561570.SS", "588510.SS", "159162.SZ",
    "589030.SS", "512700.SS", "159974.SZ", "159357.SZ", "159301.SZ", "159381.SZ", "588550.SS",
    "159140.SZ", "518880.SS", "520570.SS", "159183.SZ", "562070.SS", "551900.SS", "516820.SS",
    "562850.SS", "159223.SZ", "511200.SS", "588050.SS", "510590.SS", "515130.SS", "159730.SZ",
    "159143.SZ", "512250.SS", "159619.SZ", "510580.SS", "588040.SS", "159236.SZ", "511950.SS",
    "159062.SZ", "159956.SZ", "159173.SZ", "588690.SS", "513810.SS", "159612.SZ", "520690.SS",
    "159658.SZ", "159659.SZ", "589300.SS", "159017.SZ", "560220.SS", "517120.SS", "159985.SZ",
    "159535.SZ", "561350.SS", "588730.SS", "159358.SZ", "159919.SZ", "588850.SS", "516850.SS",
    "515810.SS", "563080.SS", "159643.SZ", "159981.SZ", "516290.SS", "551800.SS", "515530.SS",
    "563520.SS", "511820.SS", "159875.SZ", "159270.SZ", "520860.SS", "512730.SS", "520510.SS",
    "159182.SZ", "516010.SS", "510310.SS", "159677.SZ", "159515.SZ", "159126.SZ", "518850.SS",
    "512050.SS", "159215.SZ", "159680.SZ", "563750.SS", "159859.SZ", "159523.SZ", "159289.SZ",
    "159605.SZ", "515990.SS", "588930.SS", "560520.SS", "516910.SS", "159583.SZ", "159587.SZ",
    "520940.SS", "560050.SS", "159606.SZ", "159779.SZ", "515290.SS", "515330.SS", "159915.SZ",
    "561360.SS", "520880.SS", "159608.SZ", "517330.SS", "510980.SS", "520680.SS", "512010.SS",
    "589270.SS", "526050.SS", "511090.SS", "512370.SS", "560610.SS", "512480.SS", "516190.SS",
    "511270.SS", "512670.SS", "513230.SS", "159246.SZ", "561030.SS", "510680.SS", "159071.SZ",
    "159078.SZ", "513240.SS", "159283.SZ", "513170.SS", "515060.SS", "159511.SZ", "159310.SZ",
    "159993.SZ", "180901.SZ", "511110.SS", "159161.SZ", "159502.SZ", "159678.SZ", "159529.SZ",
    "159625.SZ", "515030.SS", "159331.SZ", "589350.SS", "562010.SS", "560650.SS", "159329.SZ",
    "589070.SS", "159005.SZ", "159506.SZ", "588250.SS", "562950.SS", "563670.SS", "520810.SS",
    "159171.SZ", "560020.SS", "159166.SZ", "589580.SS", "159877.SZ", "513280.SS", "159863.SZ",
    "516840.SS", "159322.SZ", "159219.SZ", "159858.SZ", "513140.SS", "563360.SS", "159131.SZ",
    "562390.SS", "513390.SS", "588430.SS", "159672.SZ", "159259.SZ", "560070.SS", "517360.SS",
    "159715.SZ", "159801.SZ", "159864.SZ", "159708.SZ", "159669.SZ", "520500.SS", "159165.SZ",
    "513830.SS", "159055.SZ", "512660.SS", "159278.SZ", "518660.SS", "159228.SZ", "515370.SS",
    "516110.SS", "512640.SS", "512070.SS", "159060.SZ", "513860.SS", "517800.SS", "159586.SZ",
    "159146.SZ", "510720.SS", "561220.SS", "513690.SS", "561260.SS", "159262.SZ", "560350.SS",
    "563660.SS", "159086.SZ", "159767.SZ", "551060.SS", "589170.SS", "563930.SS", "512170.SS",
    "516070.SS", "588280.SS", "512950.SS", "159253.SZ", "515320.SS", "159592.SZ", "513530.SS",
    "588080.SS", "563960.SS", "180401.SZ", "588260.SS", "588230.SS", "159112.SZ", "513880.SS",
    "588320.SS", "159740.SZ", "560320.SS", "561930.SS", "518680.SS", "589130.SS", "515000.SS",
    "563000.SS", "563010.SS", "512000.SS", "560620.SS", "512200.SS", "520610.SS", "159292.SZ",
    "159133.SZ", "159681.SZ", "159890.SZ", "561780.SS", "589000.SS", "588780.SS", "512260.SS",
    "159575.SZ", "516020.SS", "588070.SS", "515480.SS", "560360.SS", "159147.SZ", "159010.SZ",
    "159226.SZ", "159788.SZ", "159757.SZ", "159662.SZ", "159542.SZ", "159651.SZ", "169104.SZ",
    "561680.SS", "159577.SZ", "159839.SZ", "159909.SZ", "159691.SZ", "159050.SZ", "516330.SS",
    "588410.SS", "159557.SZ", "159211.SZ", "515860.SS", "513260.SS", "159578.SZ", "159052.SZ",
    "520990.SS", "589960.SS", "159046.SZ", "159118.SZ", "159551.SZ", "512500.SS", "159970.SZ",
    "561900.SS", "506000.SS", "159526.SZ", "513080.SS", "159709.SZ", "560400.SS", "159277.SZ",
    "510170.SS", "159805.SZ", "159129.SZ", "511190.SS", "159929.SZ", "516060.SS", "562330.SS",
    "563870.SS", "563900.SS", "589380.SS", "588960.SS", "560130.SS", "159918.SZ", "180306.SZ",
    "562810.SS", "159220.SZ", "560770.SS", "159545.SZ", "159961.SZ", "513910.SS", "551000.SS",
    "159063.SZ", "560530.SS", "159299.SZ", "159907.SZ", "513020.SS", "159392.SZ", "589080.SS",
    "510530.SS", "512750.SS", "159003.SZ", "515650.SS", "510150.SS", "511810.SS", "560780.SS",
    "159391.SZ", "512550.SS", "159706.SZ", "159885.SZ", "159203.SZ", "516600.SS", "560300.SS",
    "159363.SZ", "510800.SS", "159808.SZ", "515300.SS", "159376.SZ", "515460.SS", "589090.SS",
    "510990.SS", "511180.SS", "561590.SS", "159020.SZ", "159013.SZ", "512800.SS", "180103.SZ",
    "159191.SZ", "159375.SZ", "159949.SZ", "561100.SS", "159862.SZ", "563030.SS", "512400.SS",
    "563210.SS", "516050.SS", "159676.SZ", "159200.SZ", "159572.SZ", "515200.SS", "159857.SZ",
    "560980.SS", "159622.SZ", "560150.SS", "159533.SZ", "588360.SS", "588380.SS", "589800.SS",
    "159115.SZ", "520970.SS", "560970.SS", "588520.SS", "513870.SS", "561950.SS", "563650.SS",
    "588720.SS", "180801.SZ", "159603.SZ", "159886.SZ", "551550.SS", "560210.SS", "560860.SS",
    "159888.SZ", "562550.SS", "511360.SS", "159093.SZ", "159149.SZ", "159521.SZ", "159742.SZ",
    "515900.SS", "589200.SS", "515840.SS", "159290.SZ", "561060.SS", "159320.SZ", "159569.SZ",
    "510090.SS", "159766.SZ", "159309.SZ", "513400.SS", "510560.SS", "513090.SS", "511920.SS",
    "159069.SZ", "159867.SZ", "516550.SS", "159380.SZ", "510320.SS", "159382.SZ", "520920.SS",
    "159870.SZ", "159725.SZ", "560280.SS", "159997.SZ", "159819.SZ", "563780.SS", "159007.SZ",
    "563020.SS", "588920.SS", "516720.SS", "159330.SZ", "512620.SS", "512900.SS", "515960.SS",
    "159333.SZ", "560560.SS", "516150.SS", "511690.SS", "159305.SZ", "588150.SS", "180503.SZ",
    "159968.SZ", "159151.SZ", "159632.SZ", "159159.SZ", "159070.SZ", "560750.SS", "515550.SS",
    "588770.SS", "517160.SS", "512290.SS", "159661.SZ", "511880.SS", "159836.SZ", "159910.SZ",
    "512160.SS", "515660.SS", "501018.SS", "515910.SS", "520980.SS", "589260.SS", "563500.SS",
    "512130.SS", "159861.SZ", "517010.SS", "159116.SZ", "588660.SS", "159687.SZ", "560870.SS",
    "159589.SZ", "560590.SS", "562660.SS", "563530.SS", "506008.SS", "513180.SS", "515800.SS",
    "159176.SZ", "517850.SS", "512710.SS", "515600.SS", "159804.SZ", "588270.SS", "530280.SS",
    "159641.SZ", "515580.SS", "588180.SS", "511660.SS", "588170.SS", "512330.SS", "510290.SS",
    "169201.SZ", "159397.SZ", "510370.SS", "518890.SS", "562870.SS", "159631.SZ", "159562.SZ",
    "588900.SS", "159512.SZ", "159275.SZ", "520900.SS", "159639.SZ", "159636.SZ", "520560.SS",
    "511990.SS", "159650.SZ", "159238.SZ", "180303.SZ", "159207.SZ", "516160.SS", "513590.SS",
    "588290.SS", "516440.SS", "551510.SS", "159519.SZ", "513360.SS", "561790.SS", "589700.SS",
    "530380.SS", "517050.SS", "561380.SS", "159128.SZ", "159141.SZ", "530060.SS", "159880.SZ",
    "159235.SZ", "159821.SZ", "159841.SZ", "510760.SS", "159198.SZ", "159998.SZ", "513190.SS",
    "159216.SZ", "560160.SS", "520750.SS", "159225.SZ", "159217.SZ", "512990.SS", "159843.SZ",
    "159508.SZ", "159807.SZ", "561130.SS", "562970.SS", "159611.SZ", "516250.SS", "515680.SS",
    "588460.SS", "159936.SZ", "520700.SS", "515980.SS", "513160.SS", "169108.SZ", "169105.SZ",
    "159692.SZ",
]



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
    """捕获 handle_data 传入的最新行情快照（dict: code -> SecurityUnitData）。
    before_trading_start 的 data 参数是 StrategyUniverse（标的集合，无行情、无 __len__），
    此处仅接受 dict 形态（items 为 (code, 行情对象) 对），否则忽略。"""
    global _BARS
    if data is None:
        return
    try:
        items = data.items()
    except Exception:
        return
    out = {}
    try:
        for k, v in items:
            out[k] = v
    except Exception:
        return
    _BARS = out


def _series_values(df, security, field):
    """从单标的 get_history 结果取 field 一维数组（官方单标的列=字段名；
    本地引擎/宽表兜底列=标的码）。返回 np.ndarray(float) 或 None。"""
    if df is None or not hasattr(df, 'columns') or len(df) == 0:
        return None
    if field in df.columns:
        arr = df[field]
    elif security in df.columns:
        arr = df[security]
    elif df.shape[1] >= 1:
        arr = df.iloc[:, 0]
    else:
        return None
    try:
        return np.asarray(pd.to_numeric(arr, errors='coerce'), dtype=float)
    except Exception:
        return None


def _series_last(df, security, field):
    """从 get_history 结果取最后一个值（兼容单标的字段列 / 宽表标的码列）。"""
    vals = _series_values(df, security, field)
    if vals is None or len(vals) == 0:
        return None
    try:
        v = float(vals[-1])
    except Exception:
        return None
    return v if v == v else None  # not NaN


def _wide(df, value_col=None):
    """多标的 get_history 规整为宽表（index=时间, columns=代码）。

    官方返回格式：python3.11 长表（含 code 列，多标的统一格式）→ pivot 成宽表；
    python3.5 / 本地引擎宽表（代码列）→ 直接返回。"""
    if df is None or not isinstance(df, pd.DataFrame) or 'code' not in df.columns:
        return df
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
            pass
    return df


def _positions():
    """{PTrade码(.SS/.SZ): Position}，仅含数量>0。

    真机 get_positions() 返回键为 .XSHG/.XSHE（官方文档确认两种尾缀皆可作为键），
    归一为 .SS/.SZ 以便与策略代码（池子/目标均为 .SS/.SZ）比较。"""
    out = {}
    try:
        for code, pos in (get_positions() or {}).items():
            if not code:
                continue
            code = str(code).replace('.XSHG', '.SS').replace('.XSHE', '.SZ')
            if getattr(pos, 'amount', 0) > 0:
                out[code] = pos
    except Exception:
        pass
    return out


def _price(security, context):
    """当前价：快照 close/price → 当日最新分钟收盘 → 最近日线收盘。"""
    obj = _BARS.get(security)
    p = (getattr(obj, 'close', 0) or getattr(obj, 'price', 0) or 0) if obj else 0
    if p:
        return float(p)
    try:
        mdf = get_history(1, '1m', 'close', security_list=security, include=True)
        val = _series_last(mdf, security, 'close')
        if val:
            return val
    except Exception:
        pass
    try:
        ddf = get_history(1, '1d', 'close', security_list=security, include=True)
        val = _series_last(ddf, security, 'close')
        if val:
            return val
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
    """单标的最近日线字段值（官方单标的 get_history 列=字段名）。"""
    try:
        df = get_history(1, '1d', field, security_list=code, include=True)
        v = _series_last(df, code, field)
        if v is not None:
            return v
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
    CHUNK = 200
    chunks = list(range(0, len(codes), CHUNK))
    for ci, i in enumerate(chunks):
        chunk = list(codes)[i:i + CHUNK]
        log.info("【当日成交量查询】第%d/%d块(%d只)开始..." % (ci + 1, len(chunks), len(chunk)))
        try:
            mdf = _wide(get_history(241, '1m', 'volume', security_list=chunk, include=True), 'volume')
            if mdf is None or mdf.empty:
                log.warning("【当日成交量查询】第%d块(%d只)返回空" % (ci + 1, len(chunk)))
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


# 真机判定见 _is_real_ptrade()（get_market_list mic：本地'ALL'/真机SS/SZ）
_MONEY_CACHE = {}        # (day, count, field) -> pd.Series(code->日均成交额)，全市场；当日复用避免真机反复全市场查询


def _resolve_money_field(field):
    """money_corrected 是本地引擎专用字段（对齐聚宽口径），真机无此字段，统一回退官方 'money'。"""
    if field == 'money_corrected' and _is_real_ptrade():
        return 'money'
    return field


def _get_money_avg_series(codes, count, context, field='money'):
    """分块 get_history 拉取成交额并计算日均，返回 pd.Series(code -> 日均成交额)。
    避免对上千只标的单次 get_history 查询导致回测挂起。
    field='money_corrected'：本地引擎返回修正后的元成交额（对齐聚宽口径）；真机无该字段，
    经 _resolve_money_field 自动回退官方 'money'。当日全市场结果缓存复用（真机 get_history
    逐块查询慢，阈值与池过滤共享同一份全市场成交额，避免重复查询）。"""
    field = _resolve_money_field(field)
    day = _today(context)
    key = (day, count, field)
    cached = _MONEY_CACHE.get(key)
    if cached is not None:
        sel = cached.reindex([c for c in codes if c in cached.index])
        if len(sel) >= len(codes):
            return sel
    result = pd.Series(dtype=float)
    CHUNK = 200
    chunks = list(range(0, len(codes), CHUNK))
    for ci, i in enumerate(chunks):
        chunk = list(codes)[i:i + CHUNK]
        log.info("【成交额查询】%s 第%d/%d块(%d只)开始..." % (field, ci + 1, len(chunks), len(chunk)))
        try:
            df = _wide(get_history(count, '1d', field, security_list=chunk), field)
            if df is None or df.empty:
                log.warning("【成交额查询】第%d块(%d只)返回空" % (ci + 1, len(chunk)))
                continue
            df = df.fillna(0.0)
            avg = df.sum(axis=0) / count
            for code in chunk:
                if code in avg.index:
                    result[code] = float(avg[code])
            log.info("【成交额查询】第%d块完成，本块有成交 %d 只" % (ci + 1, int(avg.notna().sum())))
        except Exception as e:
            log.warning("【成交额查询】第%d块异常: %s" % (ci + 1, e))
            continue
    if cached is None or len(result) >= len(cached):
        _MONEY_CACHE[key] = result
    return result


def _get_money_daily_totals(codes, context):
    """按日汇总样本池成交额，返回 {日期: (总成交额, 有成交只数)}，失败返回 None。"""
    try:
        CHUNK = 200
        totals = {}
        for i in range(0, len(codes), CHUNK):
            chunk = list(codes)[i:i + CHUNK]
            df = _wide(get_history(3, '1d', 'money', security_list=chunk), 'money')
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
_MARKET_ENUM_OK = False  # 官方限制：get_market_list/get_market_detail 仅限 before/after_trading_end 内调用


_REAL_PTRADE = None  # None=未判定；真机=True，本地引擎=False


def _is_real_ptrade():
    """真机判定：本地引擎 get_market_list 返回 finance_mic='ALL'；真机返回 SS/SZ/CSI/XBHS。"""
    global _REAL_PTRADE
    if _REAL_PTRADE is None:
        try:
            ml = get_market_list()
            if ml is not None:
                for _, r in ml.iterrows():
                    mic = r.get('finance_mic') or ''
                    _REAL_PTRADE = mic != 'ALL'
                    break
            if _REAL_PTRADE is None:
                _REAL_PTRADE = False
        except Exception:
            _REAL_PTRADE = False
    return _REAL_PTRADE


def _get_all_fund_codes():
    """枚举全市场 ETF 代码/名称 {code: name}。
    优先级：1) 官方 get_etf_list()（真机交易可用，纯 ETF）；2) 内嵌 _SHIPPED_ETF_CODES
    （本地引擎生成的自洽清单，真机回测 get_etf_list 不可用、get_market_detail 枚举不可靠时兜底）。
    名称用 get_stock_name() 批量获取。失败返回 None（调用方降级）。"""
    if not _MARKET_ENUM_OK:
        return None
    src = 'get_etf_list'
    codes = None
    try:
        codes = get_etf_list()
    except Exception:
        codes = None
    if not codes:
        codes = list(_SHIPPED_ETF_CODES)
        src = '内嵌清单'
    if not codes:
        return None
    codes = [str(c) for c in codes]
    names = {}
    try:
        names = get_stock_name(codes) or {}
    except Exception:
        pass
    fund_codes = {}
    for c in codes:
        base = c.split('.')[0]
        if len(base) == 6 and base.isdigit():
            fund_codes[c] = str(names.get(c, c))
    if not fund_codes:
        return None
    log.info("全市场ETF枚举: %d 只（来源 %s）" % (len(fund_codes), src))
    return fund_codes


def _ensure_fund_universe():
    """缓存全市场基金表 g._fund_universe（{code: name}），失败则空表"""
    if getattr(g, '_fund_universe', None) is None:
        fc = _get_all_fund_codes()
        g._fund_universe = fc if fc else {}
    return g._fund_universe


# ==================== 定时任务 ====================
def initialize(context):
    try:
        set_benchmark('510300.SS')
    except Exception as e:
        log.warning('设置基准失败(仅回测有效): %s' % e)
    try:
        set_commission(commission_ratio=0.0001, min_commission=5.0, type='ETF')
        set_commission(commission_ratio=0.0001, min_commission=5.0, type='LOF')
    except Exception as e:
        log.warning('设置佣金失败(仅回测有效): %s' % e)
    try:
        set_slippage(slippage=0.0002)
    except Exception as e:
        log.warning('设置滑点失败(仅回测有效): %s' % e)

    # ==================== ETF池定义 ====================
    # 全球/海外ETF池（含大宗商品和海外市场ETF）
    g.global_etf_pool = [
        # 大宗商品ETF：
        '518880.SS',  # (黄金ETF) [ETF]-日均成交额：51.35亿元-上市日期：2013-07-29
        '501018.SS',  # (南方原油) [LOF]-日均成交额：24.38亿元-上市日期：2016-06-28
        '161226.SZ',  # (国投白银LOF) [LOF]-日均成交额：5.44亿元-上市日期：2015-08-17
        '159985.SZ',  # (豆粕ETF华夏) [ETF]-日均成交额：4.63亿元-上市日期：2019-12-05
        '159980.SZ',  # (有色ETF大成) [ETF]-日均成交额：3.84亿元-上市日期：2019-12-24
        # 海外ETF：
        '513310.SS',  # (中韩芯片) [ETF]-日均成交额：59.37亿元-上市日期：2022-12-22
        '159518.SZ',  # (标普油气ETF嘉实) [ETF]-日均成交额：27.93亿元-上市日期：2023-11-15
        '159509.SZ',  # (纳指科技ETF景顺) [ETF]-日均成交额：7.24亿元-上市日期：2023-08-08
        '513100.SS',  # (纳指ETF) [ETF]-日均成交额：5.02亿元-上市日期：2013-05-15
        '513520.SS',  # (日经ETF) [ETF]-日均成交额：3.72亿元-上市日期：2019-06-25
        '513500.SS',  # (标普500) [ETF]-日均成交额：2.89亿元-上市日期：2014-01-15
        '159502.SZ',  # (标普生物科技ETF嘉实) [ETF]-日均成交额：1.80亿元-上市日期：2024-01-10
        '513400.SS',  # (道琼斯) [ETF]-日均成交额：1.70亿元-上市日期：2024-02-02
        '513030.SS',  # (德国ETF) [ETF]-日均成交额：0.95亿元-上市日期：2014-09-05
        '513290.SS',  # (纳指生物) [ETF]-日均成交额：0.78亿元-上市日期：2022-08-29
        '520830.SS',  # (沙特ETF) [ETF]-日均成交额：0.62亿元-上市日期：2024-07-16
        '159529.SZ',  # (标普消费ETF景顺) [ETF]-日均成交额：0.50亿元-上市日期：2024-02-02
    ]
    # 中国ETF池（含港股、指数、行业ETF）
    g.china_etf_pool = [
        # 港股ETF：
        '513090.SS',  # (香港证券) [ETF]-日均成交额：54.24亿元-上市日期：2020-03-26
        '513120.SS',  # (HK创新药) [ETF]-日均成交额：52.34亿元-上市日期：2022-07-12
        '513180.SS',  # (恒指科技) [ETF]-日均成交额：36.66亿元-上市日期：2021-05-25
        '513330.SS',  # (恒生互联) [ETF]-日均成交额：20.45亿元-上市日期：2021-02-08
        '513750.SS',  # (港股非银) [ETF]-日均成交额：9.55亿元-上市日期：2023-11-27
        '159892.SZ',  # (恒生医药ETF华夏) [ETF]-日均成交额：7.90亿元-上市日期：2021-10-19
        '513190.SS',  # (H股金融) [ETF]-日均成交额：3.74亿元-上市日期：2023-10-11
        '159605.SZ',  # (中概互联ETF广发) [ETF]-日均成交额：3.19亿元-上市日期：2021-12-02
        '513630.SS',  # (香港红利) [ETF]-日均成交额：2.84亿元-上市日期：2023-12-08
        '159323.SZ',  # (港股通汽车ETF华夏) [ETF]-日均成交额：1.98亿元-上市日期：2025-01-08
        '510900.SS',  # (恒生中国) [ETF]-日均成交额：1.46亿元-上市日期：2012-10-22
        '513920.SS',  # (央企40) [ETF]-日均成交额：1.38亿元-上市日期：2024-01-05
        '513970.SS',  # (恒生消费) [ETF]-日均成交额：0.82亿元-上市日期：2023-04-21
        # 指数ETF：
        '511380.SS',  # (转债ETF) [ETF]-日均成交额：115.92亿元-上市日期：2020-04-07
        '512050.SS',  # (A500E) [ETF]-日均成交额：48.05亿元-上市日期：2024-11-15
        '510500.SS',  # (500ETF) [ETF]-日均成交额：45.45亿元-上市日期：2013-03-15
        '159915.SZ',  # (创业板ETF易方达) [ETF]-日均成交额：43.55亿元-上市日期：2011-12-09
        '510300.SS',  # (300ETF) [ETF]-日均成交额：34.60亿元-上市日期：2012-05-28
        '512100.SS',  # (1000ETF) [ETF]-日均成交额：25.26亿元-上市日期：2016-11-04
        '159949.SZ',  # (创业板50ETF华安) [ETF]-日均成交额：16.52亿元-上市日期：2016-07-22
        '588080.SS',  # (科创板50) [ETF]-日均成交额：13.32亿元-上市日期：2020-11-16
        '159967.SZ',  # (创业板成长ETF华夏) [ETF]-日均成交额：5.29亿元-上市日期：2019-07-15
        '588220.SS',  # (科创100F) [ETF]-日均成交额：5.01亿元-上市日期：2023-09-15
        '563300.SS',  # (中证2000) [ETF]-日均成交额：4.13亿元-上市日期：2023-09-14
        '510760.SS',  # (上证ETF) [ETF]-日均成交额：1.45亿元-上市日期：2020-09-09
        # 行业ETF：
        '588200.SS',  # (科创芯片) [ETF]-日均成交额：28.07亿元-上市日期：2022-10-26
        '515880.SS',  # (通信ETF) [ETF]-日均成交额：22.39亿元-上市日期：2019-09-06
        '159981.SZ',  # (能源化工ETF建信) [ETF]-日均成交额：21.63亿元-上市日期：2020-01-17
        '512880.SS',  # (证券ETF) [ETF]-日均成交额：16.21亿元-上市日期：2016-08-08
        '513350.SS',  # (油气ETF) [ETF]-日均成交额：15.66亿元-上市日期：2023-11-28
        '159326.SZ',  # (电网设备ETF华夏) [ETF]-日均成交额：14.86亿元-上市日期：2024-09-09
        '159516.SZ',  # (半导体设备ETF国泰) [ETF]-日均成交额：14.23亿元-上市日期：2023-07-27
        '159206.SZ',  # (卫星ETF永赢) [ETF]-日均成交额：13.87亿元-上市日期：2025-03-14
        '512480.SS',  # (半导体) [ETF]-日均成交额：13.07亿元-上市日期：2019-06-12
        '159363.SZ',  # (创业板人工智能ETF华宝) [ETF]-日均成交额：10.50亿元-上市日期：2024-12-16
        '159870.SZ',  # (化工ETF鹏华) [ETF]-日均成交额：10.03亿元-上市日期：2021-03-03
        '512400.SS',  # (有色ETF) [ETF]-日均成交额：9.97亿元-上市日期：2017-09-01
        '159755.SZ',  # (电池ETF广发) [ETF]-日均成交额：8.58亿元-上市日期：2021-06-24
        '588170.SS',  # (科创半导) [ETF]-日均成交额：7.74亿元-上市日期：2025-04-08
        '159992.SZ',  # (创新药ETF银华) [ETF]-日均成交额：7.59亿元-上市日期：2020-04-10
        '159995.SZ',  # (芯片ETF华夏) [ETF]-日均成交额：7.51亿元-上市日期：2020-02-10
        '512890.SS',  # (红利低波) [ETF]-日均成交额：6.79亿元-上市日期：2019-01-18
        '515220.SS',  # (煤炭ETF) [ETF]-日均成交额：6.44亿元-上市日期：2020-03-02
        '159566.SZ',  # (储能电池ETF易方达) [ETF]-日均成交额：6.31亿元-上市日期：2024-02-08
        '159819.SZ',  # (人工智能ETF易方达) [ETF]-日均成交额：6.26亿元-上市日期：2020-09-23
        '512800.SS',  # (银行ETF) [ETF]-日均成交额：6.13亿元-上市日期：2017-08-03
        '512690.SS',  # (酒ETF) [ETF]-日均成交额：5.99亿元-上市日期：2019-05-06
        '515050.SS',  # (5GETF) [ETF]-日均成交额：5.93亿元-上市日期：2019-10-16
        '562500.SS',  # (机器人) [ETF]-日均成交额：5.83亿元-上市日期：2021-12-29
        '512170.SS',  # (医疗ETF) [ETF]-日均成交额：5.63亿元-上市日期：2019-06-17
        '517520.SS',  # (黄金股) [ETF]-日均成交额：5.01亿元-上市日期：2023-11-01
        '159869.SZ',  # (游戏ETF华夏) [ETF]-日均成交额：4.77亿元-上市日期：2021-03-05
        '512070.SS',  # (证券保险) [ETF]-日均成交额：4.61亿元-上市日期：2014-07-18
        '159611.SZ',  # (电力ETF广发) [ETF]-日均成交额：4.42亿元-上市日期：2022-01-07
        '562800.SS',  # (稀有金属) [ETF]-日均成交额：4.39亿元-上市日期：2021-09-27
        '515120.SS',  # (创新药) [ETF]-日均成交额：4.34亿元-上市日期：2021-01-04
        '512010.SS',  # (医药ETF) [ETF]-日均成交额：4.27亿元-上市日期：2013-10-28
        '510880.SS',  # (红利ETF) [ETF]-日均成交额：3.97亿元-上市日期：2007-01-18
        '515790.SS',  # (光伏ETF) [ETF]-日均成交额：3.87亿元-上市日期：2020-12-18
        '515980.SS',  # (人工智能) [ETF]-日均成交额：3.78亿元-上市日期：2020-02-10
        '512660.SS',  # (军工ETF) [ETF]-日均成交额：3.75亿元-上市日期：2016-08-08
        '159928.SZ',  # (消费ETF汇添富) [ETF]-日均成交额：3.66亿元-上市日期：2013-09-16
        '512710.SS',  # (军工龙头) [ETF]-日均成交额：3.60亿元-上市日期：2019-08-26
        '560860.SS',  # (工业有色) [ETF]-日均成交额：3.57亿元-上市日期：2023-03-13
        '515030.SS',  # (新汽车) [ETF]-日均成交额：3.33亿元-上市日期：2020-03-04
        '159766.SZ',  # (旅游ETF富国) [ETF]-日均成交额：3.30亿元-上市日期：2021-07-23
        '159218.SZ',  # (卫星ETF招商) [ETF]-日均成交额：3.21亿元-上市日期：2025-05-22
        '159852.SZ',  # (软件ETF嘉实) [ETF]-日均成交额：3.19亿元-上市日期：2021-02-09
        '516160.SS',  # (新能源) [ETF]-日均成交额：3.07亿元-上市日期：2021-02-04
        '516150.SS',  # (稀土基金) [ETF]-日均成交额：3.03亿元-上市日期：2021-03-17
        '159227.SZ',  # (航空航天ETF华夏) [ETF]-日均成交额：2.98亿元-上市日期：2025-05-16
        '159583.SZ',  # (通信ETF富国) [ETF]-日均成交额：2.93亿元-上市日期：2024-07-08
        '588790.SS',  # (科创智能) [ETF]-日均成交额：2.62亿元-上市日期：2025-01-09
        '159865.SZ',  # (养殖ETF国泰) [ETF]-日均成交额：2.44亿元-上市日期：2021-03-08
        '512980.SS',  # (传媒ETF) [ETF]-日均成交额：2.43亿元-上市日期：2018-01-19
        '159851.SZ',  # (金融科技ETF华宝) [ETF]-日均成交额：2.27亿元-上市日期：2021-03-19
        '561360.SS',  # (石油ETF) [ETF]-日均成交额：2.04亿元-上市日期：2023-10-31
        '561980.SS',  # (芯片设备) [ETF]-日均成交额：2.01亿元-上市日期：2023-09-01
        '562590.SS',  # (半导材料) [ETF]-日均成交额：1.76亿元-上市日期：2023-10-18
        '512200.SS',  # (地产ETF) [ETF]-日均成交额：1.71亿元-上市日期：2017-09-25
        '159732.SZ',  # (消费电子ETF华夏) [ETF]-日均成交额：1.62亿元-上市日期：2021-08-23
        '159667.SZ',  # (工业母机ETF国泰) [ETF]-日均成交额：1.58亿元-上市日期：2022-10-26
        '516510.SS',  # (云计算) [ETF]-日均成交额：1.49亿元-上市日期：2021-04-07
        '159840.SZ',  # (锂电池ETF工银) [ETF]-日均成交额：1.42亿元-上市日期：2021-08-20
        '159998.SZ',  # (计算机ETF天弘) [ETF]-日均成交额：1.30亿元-上市日期：2020-04-13
        '159825.SZ',  # (农业ETF富国) [ETF]-日均成交额：1.15亿元-上市日期：2020-12-29
        '512670.SS',  # (国防ETF) [ETF]-日均成交额：1.12亿元-上市日期：2019-08-01
        '159883.SZ',  # (医疗器械ETF永赢) [ETF]-日均成交额：1.05亿元-上市日期：2021-04-30
        '515210.SS',  # (钢铁ETF) [ETF]-日均成交额：1.01亿元-上市日期：2020-03-02
        '515400.SS',  # (大数据) [ETF]-日均成交额：0.94亿元-上市日期：2021-01-20
        '159256.SZ',  # (创业板软件ETF华夏) [ETF]-日均成交额：0.83亿元-上市日期：2025-08-04
        '561330.SS',  # (矿业ETF) [ETF]-日均成交额：0.83亿元-上市日期：2022-11-01
        '515170.SS',  # (食品饮料) [ETF]-日均成交额：0.67亿元-上市日期：2021-01-13
        '159638.SZ',  # (高端装备ETF嘉实) [ETF]-日均成交额：0.56亿元-上市日期：2022-08-12
        '516520.SS',  # (智能驾驶) [ETF]-日均成交额：0.47亿元-上市日期：2021-03-01
        '513360.SS',  # (教育ETF) [ETF]-日均成交额：0.43亿元-上市日期：2021-06-17
        '516190.SS',  # (文娱ETF) [ETF]-日均成交额：0.18亿元-上市日期：2021-09-17
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

    g.holdings_num = 2
    g.cross_slot1_floor = 0.3          # slot1 另一资产大类动量下限
    g.cross_slot1_retain_ratio = 0.85  # slot1 保留粘性：现有同类持仓 ≥ 类首×该值保留
    g.cross_adaptive = True            # 自适应权重：强腿多得
    g.cross_weight_cap = 0.85          # slot0 权重上限
    g.target_weights = [0.5, 0.5]      # 默认双持仓等权（select_cross_asset_dual 会覆盖）
    g.defensive_etf = "511880.SS"  # 银华日利 货币ETF
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
    set_universe(list(g.fixed_etf_pool) + [g.defensive_etf])

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
    global _MARKET_ENUM_OK
    _MARKET_ENUM_OK = True
    _capture_bars(data)
    morning_routine(context)


def after_trading_end(context, data):
    """PTrade 收盘钩子（官方签名 after_trading_end(context, data)）：替代聚宽 15:10 定时任务"""
    global _MARKET_ENUM_OK
    _MARKET_ENUM_OK = True
    reset_daily_flags(context)


def handle_data(context, data):
    """盘中每分钟调用（策略回测/实盘频率需设为分钟级）：分钟级固定止损"""
    global _MARKET_ENUM_OK
    _MARKET_ENUM_OK = False
    _capture_bars(data)
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
    try:
        set_universe(list(g.merged_etf_pool) + [g.defensive_etf])
    except Exception as e:
        log.warning('set_universe 更新失败: %s' % e)
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
        for security, position in _positions().items():
            security_name = get_security_name(security)
            log.info("📊 【持仓检查】%s %s, 数量: %d, 成本: %.3f, 当前价: %.3f" % (
                security, security_name,
                int(position.amount), position.cost_basis, position.last_sale_price))
            if _is_halted(security, context):
                log.info("⚠️ %s %s 今日停牌" % (security, security_name))
    except Exception as e:
        log.warning("【持仓检查】执行异常: %s" % e)


def monitor_drawdown(context):
    try:
        current_value = context.portfolio.portfolio_value
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
                for security, position in _positions().items():
                    security_name = get_security_name(security)
                    positions_info.append("%s:%d股" % (security_name, int(position.amount)))
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
        # 阈值口径：本地引擎用全市场基金（与聚宽一致：全市场总成交额 / 除数）；
        # 真机 get_history 全市场成交额查询过慢，改用策略自有固定池（114 只）估算，保证能跑完。
        etf_list = list(g._cached_etf_universe)
        if _is_real_ptrade():
            log.info("真机模式：阈值基于全市场 ETF %d 只（get_etf_list 修正后 ~1500，分块 200）" % len(etf_list))
        if not etf_list:
            log.warning("未找到任何场内ETF，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        trade_days = get_trade_days(end_date=get_trading_day(-1), count=3)
        if len(trade_days) < 3:
            log.warning("仅有%d个有效交易日，使用保守阈值1000万" % len(trade_days))
            g.avg_etf_money_threshold = 10000000
            return
        avg_daily_money = _get_money_avg_series(etf_list, 3, context, field='money_corrected')
        if avg_daily_money.empty:
            log.warning("无成交额数据，使用保守阈值1000万")
            g.avg_etf_money_threshold = 10000000
            return
        # 分日汇总用于日志展示（真机为减少全市场查询量，跳过；本地保留）
        if not _is_real_ptrade():
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
        log.warning("计算全局阈值异常: %s，使用保守阈值1000万" % e)
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
            log.warning("【全球池过滤】无成交额数据，使用原始全球池")
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
        log.warning("【全球池过滤】异常: %s" % e)
        g.filtered_global_pool = g.global_etf_pool[:]


def update_sector_pool(context):
    log.info("【动态池更新】开始执行")
    if g.avg_etf_money_threshold is None:
        log.info("【动态池更新】阈值未初始化，立即计算")
        calculate_global_etf_threshold(context)
    if _is_real_ptrade():
        log.info("真机模式：全市场动态池按 ETF 宇宙 %d 只运行（分块 200 防 1000 块卡死）" % len(g._cached_etf_universe))
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
            log.warning("【动态池更新】无法枚举全市场基金，跳过动态池（降级为固定池）")
            g.dynamic_etf_pool = []
            return
        g.etf_names_dict = dict(fund_map)
        etf_list = list(fund_map.keys())
    except Exception as e:
        log.warning("获取全市场ETF列表失败: %s" % e)
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
            log.warning("【固定池过滤】无法获取成交额数据，跳过过滤")
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
        log.warning("【固定池过滤】异常: %s" % e)
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
    try:
        set_universe(list(g.merged_etf_pool) + [g.defensive_etf])
    except Exception as e:
        log.warning('set_universe 更新失败: %s' % e)


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
        log.debug("【指标计算】%s %s 计算失败: %s" % (etf, etf_name, e))
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
        closes = _series_values(df, code, 'close')
        if closes is None or len(closes) < data_lookback:
            log.warning("📊 【走弱期判断】%s(%s)数据不足，跳过该指数" % (name, code))
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


def get_final_ranked_etfs(context):
    all_metrics = []
    etf_set = list(g.merged_etf_pool)
    log.info("【动量得分计算】使用合并池，合计%d只ETF" % len(etf_set))
    log.info("【当前状态】%s" % ('🔴 大A走弱期' if g.is_a_share_weak else '🟢 大A正常期'))
    lookback = max(g.lookback_days, g.volume_lookback, g.ma_lookback) + 20
    today = _today(context)
    safe_lookback = lookback + 20
    close_df = _wide(get_history(safe_lookback, '1d', 'close', security_list=etf_set, fq='pre'), 'close')
    volume_df = _wide(get_history(safe_lookback, '1d', 'volume', security_list=etf_set), 'volume')
    if close_df is None or close_df.empty:
        log.warning("【动量计算】无法获取历史价格数据")
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
            if _is_halted(etf, context):
                continue
            if is_temporarily_suspended(etf, context):
                log.debug("%s %s 盘中临时停牌，跳过计算" % (etf, get_security_name(etf)))
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
            current_price = _price(etf, context)
            today_vol = today_vols.get(etf, 0)
            metrics = calculate_all_metrics_for_etf(etf, etf_name, hist_closes, hist_volumes, current_price, today_vol, context)
        except RuntimeError as e:
            skipped_no_minute.append((etf, get_security_name(etf), str(e)))
            log.warning("⚠️ %s %s 分钟数据获取失败，跳过: %s" % (etf, get_security_name(etf), e))
            continue
        if metrics:
            if metrics['etf'] in {m['etf'] for m in all_metrics}:
                continue
            all_metrics.append(metrics)
    if skipped_no_minute:
        log.warning("⚠️ 共%d只ETF因分钟数据缺失被跳过:" % len(skipped_no_minute))
        for code, name, reason in skipped_no_minute:
            log.warning("  - %s %s: %s" % (code, name, reason))
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
    current_holdings = list(_positions().keys())
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
    current_positions = _positions()
    target_set = set(target_etfs)
    sell_count = 0

    for security, position in current_positions.items():
        if position.amount > 0 and security not in target_set:
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

    current_positions = _positions()
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
        remaining_cash = context.portfolio.cash
        if remaining_cash < g.min_money:
            log.info("可用现金 %.2f 不足最小交易额 %.2f，停止买入" % (remaining_cash, g.min_money))
            break

        remaining_to_buy = num_etfs_to_buy - i
        # 槽位加权分配：新买入槽位目标市值 = 总资产 × 槽位权重(target_weights);
        # 单持仓退化 weights=[1.0] -> 全仓。最后一笔用剩余现金消化余量。
        slot = actual_holding_count + i
        total_value = context.portfolio.portfolio_value
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
        vals = _series_values(minute_data, security, 'volume')
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
        log.debug("临时停牌检测异常 %s: %s" % (security, e))
        return False  # 异常时默认认为正常，避免误判


def smart_order_target_value(security, target_value, context):
    """
    智能下单：根据目标市值调整持仓，处理停牌、涨跌停、最小交易金额、T+1
    """
    name = get_security_name(security)
    price = _price(security, context)
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
        max_shares = int(context.portfolio.cash / estimated_price)
        max_shares = (max_shares // 100) * 100
        if max_shares < target_amount:
            target_amount = max_shares
        if target_amount <= 0:
            log.info("%s %s: 现金不足买100股，跳过" % (security, name))
            return False
    else:
        target_amount = 0
    cur_pos = get_position(security)
    cur_amount = cur_pos.amount
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
        closeable = cur_pos.enable_amount
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
            log.warning("下单失败: %s %s，数量%d" % (security, name, diff))
            return False
    return False


def minute_level_stop_loss(context):
    if not g.use_fixed_stop_loss:
        return
    current_time = _current_dt(context).strftime('%H:%M')
    if not (('09:40' < current_time < '10:29') or ('10:40' < current_time < '11:30') or ('13:00' < current_time < '14:57')):
        return
    for security, position in _positions().items():
        if position.amount <= 0 or position.enable_amount <= 0:
            if security in g._profit_protected:
                del g._profit_protected[security]
            if security in g._peak_price:
                del g._peak_price[security]
            continue
        current_price = _price(security, context)
        if current_price <= 0:
            continue
        cost_price = position.cost_basis
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
    defensive_etf = g.defensive_etf
    obj = _BARS.get(defensive_etf)
    if obj is None:
        return False
    price = getattr(obj, 'close', 0) or getattr(obj, 'price', 0) or 0
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
