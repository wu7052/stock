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
filter_a = filter_fix()

"""
df_pe_grp = filter_a.filter_pe()
wx.info("[Filter PE] {} founded ".format(len(df_pe_grp)))
df_amount_grp = filter_a.filter_tt_amount()
wx.info("[Filter Total Amount] {} founded".format(len(df_amount_grp)))

df_target = pd.merge(df_pe_grp, df_amount_grp)

df_below_ma55_grp = filter_a.filter_below_ma55()
wx.info("[Filter Below Ma 55] {} founded".format(len(df_below_ma55_grp)))

df_target = pd.merge(df_target, df_below_ma55_grp)

df_high_price_grp = filter_a.filter_high_price()
wx.info("[Filter High Price] {} founded".format(len(df_high_price_grp)))

df_target = pd.merge(df_target, df_high_price_grp)

wx.info("[Filter_A Completed] {} founded".format(len(df_target)))
"""

filter_a.filter_strength()

from functions import *
# from conf import conf_handler

# update_ind_ma_2(fresh=True)
# update_ind_psy(fresh = False)
#
# update_daily_data_from_ts(period = -10)
# update_daily_data_from_eastmoney(supplement=False)
#
