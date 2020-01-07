# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
# wx.info("this is new logger")

from report_package import ws_rp
from functions import *
# from conf import conf_handler



"""####################################################################
# 申银万国行业代码导入，一次性初始化即可
"""
# update_sw_industry_code()


"""####################################################################
# 从上证、深证 网站更新 A 股基础信息
# 从申银万国 更新 行业分类
"""
# wx.info("============================[update_sh_basic_info_2]上证主板基础信息更新==========================================")
# update_sh_basic_info_2()
# wx.info("============================[update_sh_basic_info_kc]科创板基础信息更新==========================================")
# update_sh_basic_info_kc()
# wx.info("============================[update_sz_basic_info]深证主板、中小板、创业板基础信息更新==========================================")
# update_sz_basic_info()
wx.info("============================[update_sw_industry_into_basic_info]申万行业信息更新==========================================")
update_sw_industry_into_basic_info(start_from=43, start_code=None)


"""####################################################################
# 从eastmoney 获得实时交易数据 + 增补信息（换手率、量比、振幅、市盈率、市净率）
# supplement = True 表示只添加 增补信息 到数据表， = False 表示所有信息添加到数据表
"""
# 常用功能，从 eastmoney 获得全部的 日交易数据，同时更新  'cq' \ 'qfq' 两类表
wx.info("============================[update_daily_data_from_eastmoney]当日交易数据==========================================")
update_daily_data_from_eastmoney(supplement=False)

# 常用功能，获得今日需要 前复权处理的 股票id，从 tushare 获得历史 240天的 前复权数据，更新 'qfq' 表
wx.info("============================[update_last_day_qfq_data_from_ts]更新个股前复权数据==========================================")
qfq_id_arr = update_last_day_qfq_data_from_ts()

# 更新当前数据到 指定的日期
# wx.info("============================[update_daily_data_from_eastmoney(date)]==========================================")
# update_daily_data_from_eastmoney(date= '20190606',supplement=False)


"""####################################################################
# 从tushare 获取前一天的 交易数据, type = 'cq' 表示除权价格； type = 'qfq' 表示前复权价格
# 同时更新  'cq' \ 'qfq' 两类表
"""
# update_dd_by_id_from_ts(period = -14)

"""####################################################################
# 从tushare 获取指定日期的 交易数据, type = 'cq' 表示除权价格； type = 'qfq' 表示前复权价格
# 同时更新  'cq' \ 'qfq' 两类表
"""
# update_dd_by_id_from_ts(period = -1)

"""
#  计算 收盘价的 移动均值（MA5,10,20,60） 及 指数移动均值（EMA)
#  fresh = True 从过去 240个交易日 开始计算，并更新相关历史记录；在数据库初始化时使用
#  fresh = False 增量更新，最近一个交易日的均值
#  data_src='cq' 或 'qfq'，分别从除权表、前复权表 读取数据，并将结果计入不同的ma表格
"""
wx.info("============================[update_ind_ma_df]除权数据指标（增量更新）==========================================")
update_ind_ma_df(fresh=False, data_src='cq')
wx.info("============================[update_ind_ma_df]前复权数据指标（增量更新）==========================================")
update_ind_ma_df(fresh=False, data_src='qfq')
wx.info("============================[update_ind_ma_single]当日个股前复权数据指标（个股全部更新）==========================================")
update_ind_ma_single(id_arr=qfq_id_arr, data_src='qfq')


"""####################################################################
# 从 eastmoney 获得大宗交易数据
# update_whole_sales_data 先检查ws_201901大宗交易流水的最新日期，去掉最新的日期数据，因为最新日期的数据可能不完整
# 截止时间 是今天； force = True 强制更新，删除旧数据
# update_ws_share_holder 更新 share_holder 表的汇总数据
"""
wx.info("============================[update_whole_sales_data_from_eastmoney]大宗交易数据==========================================")
update_whole_sales_data_from_eastmoney(force=False)
# update_ws_share_holder()


"""####################################################################
# 从 eastmoney 获得 董高监 的交易数据（日期、股票、成交人、价格、数量、董高监、关系）
"""
wx.info("============================[update_dgj_trading_data_from_eastmoney]董高监交易数据==========================================")
update_dgj_trading_data_from_eastmoney(force= False)


"""
# 从 eastmoney 获得 股票回购 的数据
# 001：预案 ； 002: 股东大会通过 ； 003：股东大会否决
# 004：实施 ； 005：终止实施 ； 006： 实施完成
"""
wx.info("============================[update_repo_data_from_eastmoney]公司回购数据==========================================")
update_repo_data_from_eastmoney()





"""
# 更新上市公司的财务报表
# update='all'/ 'current' 分别代表更新 所有季度的报表 、当前季度的报表
# supplement = True / False 代表增量更新 、全部刷新
"""
wx.info("============================[update_fin_report_from_eastmoney]公司财报数据=====================================")
update_fin_report_from_eastmoney(update='all', supplement = True)



"""
# 最近五个交易日的 热点板块的统计图
# X坐标日期、板块名，各板块的个股数量
"""
update_hot_industry(start_date='',end_date='')
# analysis_hot_industry(duration = 5, level=1)
analysis_hot_industry(duration = 5, level=2)


"""
#  根据规则筛选股票，PE、收盘价、流通金额、Ma55、高点左右侧得分、黄金比例、
#  f_date='' 默认时间是最近交易日，或指定回测日期
#  f_name='' 指定过滤规则文件名
"""
# wx.info("============================[filter_A 选股]==========================================")
# filter_A(data_src='qfq')


"""
#  计算 心理线
#  fresh = True 从过去 60个交易日 开始计算，并更新相关历史记录；在数据库初始化时使用
#  fresh = False 增量更新，最近一个交易日的均值
"""
# update_ind_psy(fresh = False)

"""####################################################################
# 大宗交易 ws表中 补充 buy_date, sell_date，调用 mysql 存储过程
"""
# ws_supplement()


"""####################################################################
# 从sina获得交易数据
# 不指定日期，默认是当天
# 指定日期，sina 的交易数据更新到 指定日期
"""
# update_daily_data_from_sina()
# update_daily_data_from_sina(date = '20190201')


"""####################################################################
# 调用mysql 存储过程获得 A 股市值
"""
# get_list_a_total_amount()



"""
# 数据分析结果导入fruit表
"""
# analysis_dgj()
# analysis_repo()
# analysis_ws()

"""####################################################################
# 报表输出功能
"""
# report_total_amount()

# reporter = ws_rp()
#
# id_arr = analysis_summary_list(rp=reporter)
# analysis_single_stock(rp=reporter, id_arr = id_arr)

# 董高监 交易数据，最新100条
# report_dgj_trading(rp=reporter, limit=100)

# 大宗交易 最近N天 个股买入量、均价、最高价、最低价
# report_ws_price(rp=reporter, days=200)

# 最近N天 个股成交量占比 =  累计成交量 / 流动股份
# report_days_vol(rp=reporter, days=30)


# 废弃函数
# 大宗交易 董高监交易 交叉对比
# report_cross_dgj_ws(rp=reporter, ws_days=180, dgj_days=180)

# 股票回购
# report_repo_completion_data(rp= reporter)