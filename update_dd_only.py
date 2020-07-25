# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from functions import *

# 常用功能，从 eastmoney 获得全部的 日交易数据，同时更新  'cq' \ 'qfq' 两类表
wx.info("============================[update_daily_data_from_eastmoney]当日交易数据==========================================")
update_daily_data_from_eastmoney(supplement=False)

# 常用功能，获得今日需要 前复权处理的 股票id，从 tushare 获得历史 240天的 前复权数据，更新 'qfq' 表
wx.info("============================[update_last_day_qfq_data_from_ts]更新个股前复权数据==========================================")
qfq_id_arr = update_last_day_qfq_data_from_ts()

"""
# 最近五个交易日的 热点板块的统计图
# X坐标日期、板块名，各板块的个股数量
"""
update_hot_industry(start_date='',end_date='')

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


