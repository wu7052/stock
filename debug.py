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


# update_hot_industry(start_date='',end_date='')
# analysis_hot_industry(duration = 5, level=2)

update_fin_report_from_eastmoney()

# f_industry = filter_industry( f_conf='filter_rules\\filter_001.conf', data_src='qfq', industry_name='仪器仪表')
# df_ma_up_grp = f_industry.filter_ma_up()
# df_c_up_ma10_grp = f_industry.filter_c_up_ma10(df_grp = df_ma_up_grp)
# df_ws_grp = f_industry.filter_ws_record(df_grp=df_c_up_ma10_grp, duration=-30)
# print(df_ws_grp)



# analysis_hot_industry(duration = 5, level=1)
# analysis_hot_industry(duration = 5, level=2)

# update_dgj_trading_data_from_eastmoney(force= False)
# update_whole_sales_data_from_eastmoney(force=False)

