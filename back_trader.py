# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ma_kits, psy_kits
from report_package import ws_rp

import pandas as pd
from filter_package import filter_fix
from assess_package import back_trader
from functions import *

bt = back_trader(f_end_date='20190130', f_b_days=-240,  f_name='filter_rules\\filter_001.conf')
# wx.info("============================[bt.clear_bt_data]清除回测动态前复权数据==========================================")
# bt.clear_bt_data()
# wx.info("============================[bt.get_qfq_data]重建回测动态前复权数据==========================================")
# bt.get_qfq_data()
#
# wx.info("============================[update_ind_ma_df]重建回测数据指标==========================================")
# update_ind_ma_df(fresh=True, data_src='bt_qfq', bt_start_date=bt.f_begin_date, bt_end_date=bt.f_end_date)

wx.info("============================[filter_A] 使用动态前复权数据选股 ==========================================")
filter_A(data_src='bt_qfq', f_name='filter_rules\\filter_001.conf', f_start_date=bt.f_begin_date, f_end_date=bt.f_end_date)

