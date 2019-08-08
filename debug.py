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
from filter_package import filter_fix
from assess_package import back_trader
from functions import *

# bt = back_trader(f_date='20190130', f_b_days=-100,  f_name='filter_rules\\filter_001.conf')
# bt.clear_bt_data()
# bt.get_qfq_data()


# update_sh_basic_info_kc()
# update_sh_basic_info_2()

# filter_A(data_src='cq')
filter_A(data_src='qfq')
# update_daily_data_from_ts(period = -1)
# wx.info("============================[update_last_day_qfq_data_from_ts]更新个股前复权数据==========================================")
# qfq_id_arr = update_last_day_qfq_data_from_ts(start= -1)

# update_daily_data_from_eastmoney(supplement=False)
# update_daily_data_from_eastmoney(date= '20190712',supplement=False)
# update_daily_data_from_ts(period = -240, type='qfq')
# qfq_id_arr = update_last_day_qfq_data_from_ts()
# update_ind_ma_single(id_arr=qfq_id_arr, data_src='qfq')

# update_daily_qfq_data_from_ts()
# from conf import conf_handler

# update_ind_ma_2(fresh=True)
# update_ind_psy(fresh = False)
#
# update_daily_data_from_ts(period = -10)
# update_daily_data_from_eastmoney(supplement=False)
#
# wx.info("============================[update_ind_ma_df]除权数据指标（增量更新）==========================================")
# update_ind_ma_df(fresh=True, data_src='cq')
# wx.info("============================[update_ind_ma_df]前复权数据指标（增量更新）==========================================")
# update_ind_ma_df(fresh=True, data_src='qfq')
# wx.info("============================[update_ind_ma_single]当日个股前复权数据指标（个股全部更新）==========================================")
# update_ind_ma_single(id_arr=qfq_id_arr, data_src='qfq')
