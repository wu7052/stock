# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from functions import *

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


"""
# 最近五个交易日的 热点板块的统计图
# X坐标日期、板块名，各板块的个股数量
"""
update_hot_industry(start_date='',end_date='')