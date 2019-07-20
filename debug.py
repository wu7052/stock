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

from functions import *

filter_A(data_src='cq')
filter_A(data_src='qfq')
# update_daily_data_from_eastmoney(supplement=False)
# update_daily_data_from_eastmoney(date= '20190712',supplement=False)
# update_daily_data_from_ts(period = -240, type='qfq')
# update_last_day_qfq_data_from_ts()
# update_daily_qfq_data_from_ts()
# from conf import conf_handler

# update_ind_ma_2(fresh=True)
# update_ind_psy(fresh = False)
#
# update_daily_data_from_ts(period = -10)
# update_daily_data_from_eastmoney(supplement=False)
#
