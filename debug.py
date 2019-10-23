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

# update_fin_report_from_eastmoney(update='all', supplement = True)
# update_hot_industry(start_date='',end_date='')
# analysis_hot_industry(duration = 5, level=1)
# analysis_hot_industry(duration = 5, level=2)
wx.info("============================[update_fin_report_from_eastmoney]公司财报数据=====================================")
update_fin_report_from_eastmoney(update='all', supplement = True)


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

