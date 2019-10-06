# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ma_kits, psy_kits
from report_package import ws_rp
# ma = ma_kits()
# psy = psy_kits()
# np_cprice = psy.get_cprice(stock_id= "600000")
# psy.calc(np_cprice)

import pandas as pd
from filter_package import filter_fix, filter_industry
from assess_package import back_trader
from functions import *

wx.info("============================[update_last_day_qfq_data_from_ts]更新个股前复权数据==========================================")
qfq_id_arr = update_last_day_qfq_data_from_ts()
wx.info("============================[update_ind_ma_single]当日个股前复权数据指标（个股全部更新）==========================================")
update_ind_ma_single(id_arr=qfq_id_arr, data_src='qfq')


"""
# 最近五个交易日的 热点板块的统计图
# X坐标日期、板块名，各板块的个股数量
"""
update_hot_industry(start_date='',end_date='')
# analysis_hot_industry(duration = 5, level=1)
analysis_hot_industry(duration = 5, level=2)
# update_hot_industry(start_date='',end_date='')
# analysis_hot_industry(duration = 5, level=2)

# update_fin_report_from_eastmoney(update='current', supplement = True)

# f_industry = filter_industry( f_conf='filter_rules\\filter_001.conf', data_src='qfq', industry_name='计算机应用')
# df_ma_up_grp = f_industry.filter_ma_up() # id, date, ma_5, ma_10, ma_20, ma_60
# df_c_up_ma10_grp = f_industry.filter_c_up_ma10(df_grp = df_ma_up_grp) # id , date_x, ma_5, ma_10, ma_20, ma_60, data_y, close
# df_ws_grp = f_industry.filter_ws_record(df_grp=df_c_up_ma10_grp, duration=-30) # id, ave_disc, max_disc, min_disc, t_amount, t_vol
# print(df_ws_grp)

# 財務報表作為 剔除條件
# df_roe_ave = f_industry.filter_fin_roe_top()  # id, roe



# analysis_hot_industry(duration = 5, level=1)
# analysis_hot_industry(duration = 5, level=2)

# update_dgj_trading_data_from_eastmoney(force= False)
# update_whole_sales_data_from_eastmoney(force=False)

