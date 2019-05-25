# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ts_data, sz_web_data, sh_web_data, ex_web_data, ma_kits
from report_package import ws_rp
from functions import *
# from conf import conf_handler
# update_sw_industry_into_basic_info()
# ma = ma_kits()
#
# ma_ret = ma.calc(stock_id="600000", fresh=False)
# pass
# update_sh_basic_info_2()
# update_daily_data_from_ts(period = -55)

update_ind_ma(fresh=False)

# update_daily_data_from_eastmoney(supplement=False)
