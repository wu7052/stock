# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
# wx.info("this is new logger")

from report_package import ws_rp
from functions import *

"""####################################################################
# 申银万国行业代码导入，一次性初始化即可
"""
# update_sw_industry_code()


"""####################################################################
# 从上证、深证 网站更新 A 股基础信息
# 从申银万国 更新 行业分类
"""
# update_sh_basic_info()
# update_sz_basic_info()
# update_sw_industry_into_basic_info()


"""####################################################################
# 从eastmoney 获得实时交易数据 + 增补信息（换手率、量比、振幅、市盈率、市净率）
# supplement = True 表示只添加 增补信息 到数据表， = False 表示所有信息添加到数据表
"""
# 常用功能，从 eastmoney 获得全部的 日交易数据
# update_daily_data_from_eastmoney(supplement=False)

# 更新当前数据到 指定的日期
# update_daily_data_from_eastmoney(date= '20190202',supplement=False)


"""####################################################################
# 从 eastmoney 获得大宗交易数据
# update_whole_sales_data 先检查ws_201901大宗交易流水的最新日期，截止时间 是今天； force = True 强制更新，删除旧数据
# update_ws_share_holder 更新 share_holder 表的汇总数据
"""
# update_whole_sales_data_from_eastmoney(force=False)
# update_ws_share_holder()


"""####################################################################
# 从 eastmoney 获得 董高监 的交易数据（日期、股票、成交人、价格、数量、董高监、关系）
"""
update_dgj_trading_data_from_eastmoney(force= False)


"""
# 从 eastmoney 获得 股票回购 的数据
"""
# update_repo_data_from_eastmoney(force = False, start_date=None)



"""####################################################################
# 调用 mysql 存储过程，在 ws表中 补充 stock_name , h_name, buy_date, sell_date
"""
# ws_supplement()


"""####################################################################
# 从sina获得实时的交易数据
# 不指定日期，默认是当天
# 指定日期，sina 的交易数据更新到 指定日期
"""
# update_daily_data_from_sina()
# update_daily_data_from_sina(date = '20190201')


"""####################################################################
# 调用mysql 存储过程获得 A 股市值
"""
#get_list_a_total_amount()


"""####################################################################
# 从tushare 获取前一天的 交易数据
"""
# update_daily_data_from_ts(period = -1)


"""####################################################################
# 报表输出功能
"""
# report_total_amount()
reporter = ws_rp()

# 董高监 交易数据，最新100条
# report_dgj_trading(rp=reporter, limit=100)

# 大宗交易 最近N天 个股买入量、均价、最高价、最低价
# report_ws_price(rp=reporter, days=200)

# 最近N天 个股成交量占比 =  累计成交量 / 流动股份
# report_days_vol(rp=reporter, days=30)

# 大宗交易 董高监交易 交叉对比
# report_cross_dgj_ws(rp=reporter, ws_days=180, dgj_days=180)